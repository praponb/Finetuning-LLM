SELECT id, status, discount
FROM (
    SELECT
        id,
        status,
        discount,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY discount) AS row_num
    FROM orders
) AS ranked
WHERE row_num <= 5
ORDER BY status
