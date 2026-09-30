SELECT
    id,
    region,
    opened_date,
    sqft,
    LAG(sqft, 2) OVER (PARTITION BY region ORDER BY opened_date, id) AS prev_sqft
FROM stores
WHERE sqft <> 8478;
