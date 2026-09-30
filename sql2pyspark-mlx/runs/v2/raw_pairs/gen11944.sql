SELECT id, region, opened_date, sqft, LEAD(sqft, 2) OVER (PARTITION BY region ORDER BY opened_date, id) AS next_sqft FROM stores WHERE sqft > 12975 ORDER BY sqft;
