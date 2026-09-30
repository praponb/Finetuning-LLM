SELECT store_id, SUM(revenue) AS total_rev FROM store_sales GROUP BY store_id HAVING SUM(revenue) >= 10000;
