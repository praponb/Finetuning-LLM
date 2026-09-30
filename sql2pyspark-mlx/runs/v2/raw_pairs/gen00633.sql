SELECT product_id, SUM(revenue) AS total_revenue, SUM(units) AS total_units FROM store_sales WHERE store_id IN (1, 3, 4) GROUP BY product_id HAVING total_revenue < 18000 ORDER BY product_id;
