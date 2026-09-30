SELECT product_id, SUM(revenue) AS total_revenue FROM store_sales WHERE product_id = 8 AND (store_id IN (1, 2, 5) OR store_id IN (3, 5, 6)) GROUP BY product_id;
