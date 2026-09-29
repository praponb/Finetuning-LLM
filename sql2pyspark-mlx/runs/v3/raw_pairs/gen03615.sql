SELECT
    id,
    unit_price,
    product_id,
    quantity
FROM order_items
WHERE product_id IN (28, 36, 39)
