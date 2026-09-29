SELECT
    id,
    opened_date,
    region,
    sqft
FROM stores
WHERE city IN ('London', 'Tokyo', 'Berlin')
ORDER BY 4, 3
