SELECT id, opened_date, sqft, LEAD(sqft, 2) OVER (ORDER BY opened_date, id) AS next_sqft FROM stores
