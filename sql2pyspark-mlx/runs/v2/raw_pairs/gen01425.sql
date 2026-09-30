SELECT
    id,
    region,
    opened_date,
    city,
    sqft,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date, id) AS rn,
    LAG(sqft, 1, 0) OVER (PARTITION BY city ORDER BY opened_date, id) AS prev_sqft
FROM stores
