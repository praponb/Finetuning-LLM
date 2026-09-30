SELECT
    id,
    segment,
    age,
    country,
    DENSE_RANK() OVER (PARTITION BY segment ORDER BY age) AS age_dense_rank,
    COUNT(*) OVER (PARTITION BY country) AS country_count
FROM customers
ORDER BY country;
