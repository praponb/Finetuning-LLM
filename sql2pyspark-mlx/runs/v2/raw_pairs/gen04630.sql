SELECT
    id,
    order_id,
    quantity,
    unit_price
FROM (
    SELECT
        id,
        order_id,
        quantity,
        unit_price,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity DESC, id) AS pos
    FROM order_items
) AS r
WHERE pos <= 2
