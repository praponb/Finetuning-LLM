SELECT
    order_items.id,
    order_items.quantity,
    products.brand,
    products.stock_qty
FROM order_items
INNER JOIN products ON order_items.product_id = products.id
ORDER BY id DESC
