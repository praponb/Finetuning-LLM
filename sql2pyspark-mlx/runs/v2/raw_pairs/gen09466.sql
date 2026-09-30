WITH Ranked AS (
    SELECT
        id,
        customer_id,
        discount,
        amount,
        RANK() OVER (PARTITION BY customer_id ORDER BY discount) AS rnk
    FROM orders
)
SELECT
    id,
    customer_id,
    discount,
    amount
FROM Ranked
WHERE rnk <= 5
