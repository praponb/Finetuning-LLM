SELECT
    id,
    unit_price,
    product_id,
    quantity
FROM order_items
WHERE unit_price > (SELECT MAX(unit_price) FROM order_items WHERE order_id = 16)
ORDER BY product_id DESC
