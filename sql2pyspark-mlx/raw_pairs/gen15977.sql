SELECT store_id, MAX(units) - MIN(units) AS units_range FROM store_sales GROUP BY 1
