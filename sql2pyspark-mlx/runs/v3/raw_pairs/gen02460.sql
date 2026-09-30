SELECT id, region, city, opened_date, sqft, LEAD(sqft, 1) OVER (PARTITION BY region, city ORDER BY opened_date, id) AS next_sqft FROM stores WHERE region = 'South'
