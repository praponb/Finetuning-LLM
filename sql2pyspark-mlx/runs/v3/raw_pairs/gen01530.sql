SELECT
    id,
    country,
    rating,
    DENSE_RANK() OVER (PARTITION BY country ORDER BY rating) AS rating_dense_rank,
    COUNT(*) OVER (PARTITION BY country) AS country_count
FROM suppliers;
