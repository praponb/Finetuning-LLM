SELECT
    id,
    region,
    opened_date,
    sqft,
    SUM(sqft) OVER (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sqft
FROM stores
