SELECT
    id,
    country,
    age,
    segment,
    MAX(age) OVER (PARTITION BY country) AS country_max_age,
    COUNT(*) OVER (PARTITION BY segment) AS segment_count
FROM customers
WHERE credit_limit <= 12900
ORDER BY segment_count ASC
