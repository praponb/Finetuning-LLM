SELECT
    id,
    country,
    rating,
    RANK() OVER (PARTITION BY country ORDER BY rating) AS rnk
FROM suppliers
