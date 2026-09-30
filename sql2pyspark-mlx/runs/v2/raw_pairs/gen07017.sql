WITH TopOrderItems AS (
    SELECT
        id,
        order_id,
        unit_price,
        product_id,
        RANK() OVER (PARTITION BY order_id ORDER BY unit_price) AS pos
    FROM order_items
    WHERE product_id = 29
)
SELECT
    id,
    order_id,
    unit_price,
    product_id
FROM TopOrderItems
WHERE pos = 1
ORDER BY id;
