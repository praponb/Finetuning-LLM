SELECT
    id,
    country,
    rating,
    RANK() OVER (PARTITION BY country ORDER BY rating DESC) AS rating_rank
FROM suppliers
