SELECT
    id,
    product_id,
    quantity,
    unit_price,
    quantity - LAG(quantity, 1) OVER (PARTITION BY product_id ORDER BY quantity, id) quantity_change,
    quantity - LAG(quantity, 2, 0) OVER (PARTITION BY product_id ORDER BY unit_price, id) quantity_change_2
FROM order_items
WHERE quantity <> 3
ORDER BY quantity DESC
