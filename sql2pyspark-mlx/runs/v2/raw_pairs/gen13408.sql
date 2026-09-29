SELECT id, region, opened_date, sqft, sqft - LAG(sqft, 1, 0) OVER (PARTITION BY region ORDER BY opened_date, id) AS sqft_change FROM stores ORDER BY sqft DESC;
