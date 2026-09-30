WITH Ranked AS (
    SELECT
        id,
        customer_id,
        amount,
        status,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY amount) AS row_num
    FROM orders
)
SELECT
    id,
    customer_id,
    amount,
    status
FROM Ranked
WHERE row_num <= 3;
