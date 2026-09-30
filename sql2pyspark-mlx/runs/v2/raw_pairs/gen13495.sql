SELECT product_id, MIN(revenue) AS min_revenue FROM store_sales GROUP BY product_id ORDER BY min_revenue DESC, product_id LIMIT 5;
