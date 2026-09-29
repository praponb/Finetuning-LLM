SELECT id, city, opened_date, sqft, LAG(sqft, 1) OVER (PARTITION BY city ORDER BY opened_date, id) AS prev_sqft FROM stores WHERE region IS NOT NULL
