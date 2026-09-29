WITH top_order_items AS (
    SELECT
        id,
        order_id,
        quantity,
        DENSE_RANK() OVER (PARTITION BY order_id ORDER BY quantity DESC) AS row_num
    FROM order_items
)
SELECT id, order_id, quantity
FROM top_order_items
WHERE row_num <= 5;
