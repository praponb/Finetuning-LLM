SELECT id, city, opened_date, sqft, sqft - LAG(sqft, 2) OVER (PARTITION BY city ORDER BY opened_date, id) sqft_change FROM stores
