SELECT order_items.id, order_items.unit_price, order_items.quantity
FROM order_items
WHERE order_items.product_id IN (SELECT id FROM products WHERE product_name LIKE '%mous%')
ORDER BY unit_price DESC, id DESC
