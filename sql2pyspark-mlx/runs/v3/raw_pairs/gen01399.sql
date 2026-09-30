SELECT
    id,
    segment,
    age,
    MIN(age) OVER (PARTITION BY segment) AS segment_min_age
FROM customers
WHERE city IN ('Singapore', 'Tokyo', 'Bangkok')
