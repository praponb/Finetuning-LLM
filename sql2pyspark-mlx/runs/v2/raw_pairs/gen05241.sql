SELECT id, opened_date, sqft, LEAD(sqft, 1, 0) OVER (ORDER BY opened_date, id) AS next_sqft FROM stores ORDER BY id DESC
