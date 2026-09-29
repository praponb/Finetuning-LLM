WITH ranked AS (
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        DENSE_RANK() OVER (PARTITION BY order_id ORDER BY quantity DESC) row_num
    FROM order_items
    WHERE quantity != 4
)
SELECT
    id,
    order_id,
    quantity,
    unit_price
FROM ranked
WHERE row_num = 1
ORDER BY unit_price DESC, order_id DESC;
