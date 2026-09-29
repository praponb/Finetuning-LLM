SELECT store_id, MAX(units) AS max_units FROM store_sales GROUP BY 1;
