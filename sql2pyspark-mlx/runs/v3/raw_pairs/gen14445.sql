WITH ranked_orders AS (
    SELECT
        id,
        customer_id,
        amount,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC, id) AS pos
    FROM orders
)
SELECT id, customer_id, amount
FROM ranked_orders
WHERE pos <= 2;
