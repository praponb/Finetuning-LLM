SELECT ROUND(AVG(units), 2) AS avg_units, COUNT(units) AS units_count FROM store_sales WHERE store_id = 3 AND sale_date >= '2023-06-01' ORDER BY units_count
