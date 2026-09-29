SELECT
    id,
    order_id,
    unit_price,
    quantity
FROM (
    SELECT
        id,
        order_id,
        unit_price,
        quantity,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY unit_price DESC, id) AS rn
    FROM order_items
    WHERE quantity <= 4
) AS ranked
WHERE rn <= 2
