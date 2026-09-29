SELECT id, city, opened_date, sqft, sqft - LAG(sqft, 1, 0) OVER (PARTITION BY city ORDER BY opened_date, id) AS sqft_change FROM stores ORDER BY city
