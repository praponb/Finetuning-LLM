WITH ranked AS (
    SELECT
        id,
        order_id,
        quantity,
        product_id,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity DESC, id) AS pos
    FROM order_items
    WHERE order_id IN (22, 28)
)
SELECT
    id,
    order_id,
    quantity,
    product_id
FROM ranked
WHERE pos = 1;
