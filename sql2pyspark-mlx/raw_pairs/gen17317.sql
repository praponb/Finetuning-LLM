SELECT
    id,
    country,
    rating,
    DENSE_RANK() OVER (PARTITION BY country ORDER BY rating DESC) AS dense_rnk
FROM suppliers;
