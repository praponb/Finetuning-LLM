SELECT *
FROM (
    SELECT
        id,
        customer_id,
        discount,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY discount DESC, id) AS row_num
    FROM orders
) AS r
WHERE row_num <= 5
ORDER BY row_num DESC, customer_id ASC
