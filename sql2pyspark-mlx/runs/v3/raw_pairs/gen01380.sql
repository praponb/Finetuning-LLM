SELECT product_id, SUM(units) total_units FROM store_sales WHERE product_id IN (4, 6, 9, 10) AND units >= 35 GROUP BY product_id ORDER BY product_id DESC;
