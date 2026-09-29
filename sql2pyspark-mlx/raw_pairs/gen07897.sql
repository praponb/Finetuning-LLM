SELECT product_id, unique_stores FROM (SELECT product_id, COUNT(DISTINCT revenue) AS unique_stores FROM store_sales GROUP BY product_id) AS sub WHERE unique_stores < 6 ORDER BY product_id DESC;
