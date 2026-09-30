SELECT id, customer_id, amount
FROM (
    SELECT
        id,
        customer_id,
        amount,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY amount DESC) AS row_num
    FROM orders
) AS r
WHERE row_num <= 2
ORDER BY customer_id DESC
