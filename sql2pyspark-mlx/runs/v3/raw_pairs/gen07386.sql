SELECT id, opened_date, sqft, region, SUM(sqft) OVER (ORDER BY opened_date, id) AS running_sqft, LEAD(sqft, 2) OVER (PARTITION BY region ORDER BY opened_date, id) AS next_sqft FROM stores
