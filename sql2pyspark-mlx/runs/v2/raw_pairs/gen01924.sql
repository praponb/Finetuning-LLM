SELECT
    order_items.id,
    order_items.quantity,
    order_items.unit_price,
    order_items.product_id,
    products.category,
    products.cost,
    products.brand
FROM order_items
LEFT JOIN products ON order_items.product_id = products.id
ORDER BY quantity ASC
