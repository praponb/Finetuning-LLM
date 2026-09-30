SELECT store_id, MIN(units) AS min_units FROM store_sales WHERE store_id IN (4, 5) AND units >= 22 GROUP BY store_id ORDER BY store_id DESC
