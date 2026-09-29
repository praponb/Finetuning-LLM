SELECT
    order_items.product_id,
    products.product_name,
    products.stock_qty,
    products.category
FROM order_items
FULL OUTER JOIN products ON order_items.product_id = products.id
WHERE products.cost >= 610;
