SELECT
    id,
    city,
    opened_date,
    sqft,
    FIRST_VALUE(sqft) OVER (PARTITION BY city ORDER BY opened_date DESC, id) AS first_sqft
FROM stores
