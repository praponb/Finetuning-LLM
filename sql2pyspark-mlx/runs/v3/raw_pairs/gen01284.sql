SELECT
    id,
    order_id,
    quantity,
    unit_price,
    ROUND(unit_price, 1) AS unit_price_rounded
FROM order_items
WHERE quantity >= 7
ORDER BY id, unit_price DESC
