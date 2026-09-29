SELECT id, city, opened_date, sqft, LAG(sqft, 1, 0) OVER (PARTITION BY city ORDER BY opened_date, id) AS prev_sqft FROM stores;
