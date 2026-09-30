SELECT
    id,
    country,
    rating,
    DENSE_RANK() OVER (PARTITION BY country ORDER BY rating DESC) AS rating_dense_rank
FROM suppliers
WHERE supplier_name LIKE 'Eur%'
