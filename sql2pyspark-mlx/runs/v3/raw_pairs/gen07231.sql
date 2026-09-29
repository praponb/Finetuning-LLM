SELECT
    id,
    city,
    opened_date,
    sqft,
    LAG(sqft, 1, 0) OVER (PARTITION BY city ORDER BY opened_date, id) AS prev_sqft,
    ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 3) AS sqft_share
FROM stores
ORDER BY sqft DESC;
