WITH ranked_orders AS (
    SELECT
        id,
        store_id,
        order_date,
        DENSE_RANK() OVER (PARTITION BY store_id ORDER BY order_date DESC) AS pos
    FROM orders
    WHERE customer_id = 27
)
SELECT *
FROM ranked_orders
WHERE pos <= 5
ORDER BY id DESC
