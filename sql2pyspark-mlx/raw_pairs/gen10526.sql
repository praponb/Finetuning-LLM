SELECT
    id,
    opened_date,
    sqft,
    LAG(sqft, 1) OVER (ORDER BY opened_date, id) AS prev_sqft
FROM stores
