SELECT tag, COUNT(*) AS product_count FROM products LATERAL VIEW EXPLODE(tags) t AS tag WHERE ARRAY_CONTAINS(tags, 'premium') GROUP BY tag;
