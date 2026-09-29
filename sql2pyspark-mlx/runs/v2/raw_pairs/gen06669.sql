SELECT
    id,
    unit_price,
    product_id,
    quantity
FROM order_items
WHERE unit_price = (SELECT MIN(unit_price) FROM order_items WHERE product_id IN (26, 38))
ORDER BY quantity
