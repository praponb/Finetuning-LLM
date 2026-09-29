SELECT id, city, opened_date, sqft, SUM(sqft) OVER (PARTITION BY city ORDER BY opened_date, id) running_sqft FROM stores WHERE region = 'North';
