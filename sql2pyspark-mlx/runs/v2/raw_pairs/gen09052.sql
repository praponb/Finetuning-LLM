SELECT store_id, MIN(units) AS min_units FROM store_sales GROUP BY store_id
