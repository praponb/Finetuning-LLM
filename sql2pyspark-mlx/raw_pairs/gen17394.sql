SELECT
    id,
    city,
    opened_date,
    DENSE_RANK() OVER (PARTITION BY city ORDER BY opened_date) AS opened_date_dense_rank
FROM stores;
