SELECT
    id,
    region,
    opened_date,
    sqft,
    LAG(sqft, 2, 0) OVER (PARTITION BY region ORDER BY opened_date, id) AS prev_sqft,
    LAG(sqft, 1) OVER (ORDER BY opened_date, id) AS prev_sqft_2
FROM stores
ORDER BY region ASC
