SELECT
    id,
    city,
    sqft,
    region,
    LAST_VALUE(region) OVER (PARTITION BY city ORDER BY sqft DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_region
FROM stores
WHERE opened_date < '2025-03-01'
