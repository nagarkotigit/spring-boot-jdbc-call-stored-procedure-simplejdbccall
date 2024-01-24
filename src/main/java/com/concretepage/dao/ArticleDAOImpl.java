package com.concretepage.dao;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.jdbc.core.simple.SimpleJdbcCall;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.concretepage.entity.Article;
import com.concretepage.entity.ArticleRowMapper;

@Transactional
@Repository
public class ArticleDAOImpl implements IArticleDAO {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public Article getArticleById(int articleId) {
        SimpleJdbcCall simpleJdbcCall = new SimpleJdbcCall(jdbcTemplate);
        SqlParameterSource in = new MapSqlParameterSource().addValue("in_id", articleId);
        Map<String, Object> out = simpleJdbcCall.withProcedureName("read_article").execute(in);
        Article article = new Article();
        article.setCategory(out.get("out_category").toString());
        article.setTitle(out.get("out_title").toString());
        article.setArticleId(articleId);
        return article;
    }
    @Override
    public Article getArticleByParam(String title, String category) {
        SimpleJdbcCall simpleJdbcCall = new SimpleJdbcCall(jdbcTemplate);
        SqlParameterSource in = new MapSqlParameterSource()
                .addValue("in_title", title)
                .addValue("in_category", category);

        Map<String, Object> out = simpleJdbcCall.withProcedureName("getArticleByParameters").execute(in);
        Article article = new Article();
        article.setCategory(out.get("out_category").toString());
        article.setTitle(out.get("out_title").toString());
        article.setArticleId((Integer) out.get("out_article_id"));
        return article;
    }
    @Override
    public List<Article> getAllArticles() {
        String sql = "SELECT article_id, title, category FROM articles";
        // RowMapper<Article> rowMapper = new
        // BeanPropertyRowMapper<Article>(Article.class);
        RowMapper<Article> rowMapper = new ArticleRowMapper();
        return this.jdbcTemplate.query(sql, rowMapper);
    }

    @Override
    public void addArticle(Article article) {
        SimpleJdbcCall simpleJdbcCall = new SimpleJdbcCall(jdbcTemplate);
        SqlParameterSource in = new MapSqlParameterSource()
                .addValue("in_title", article.getTitle())
                .addValue("in_category", article.getCategory());
        simpleJdbcCall.withProcedureName("add_article").execute(in);

        // Fetch article id
        String sql = "SELECT article_id FROM articles WHERE title = ? and category=?";
        int articleId = jdbcTemplate.queryForObject(sql, Integer.class, article.getTitle(), article.getCategory());
        article.setArticleId(articleId);
    }

    @Override
    public void updateArticle(Article article) {
        String sql = "UPDATE articles SET title=?, category=? WHERE article_id=?";
        jdbcTemplate.update(sql, article.getTitle(), article.getCategory(), article.getArticleId());
    }

    @Override
    public void deleteArticle(int articleId) {
        String sql = "DELETE FROM articles WHERE article_id=?";
        jdbcTemplate.update(sql, articleId);
    }

    @Override
    public boolean articleExists(String title, String category) {
        String sql = "SELECT count(*) FROM articles WHERE title = ? and category=?";
        int count = jdbcTemplate.queryForObject(sql, Integer.class, title, category);
        if (count == 0) {
            return false;
        } else {
            return true;
        }
    }


}