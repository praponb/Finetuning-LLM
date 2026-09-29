SELECT
    products.id,
    products.category,
    COUNT(order_items.id) order_item_count,
    MIN(order_items.quantity) min_quantity
FROM products
LEFT OUTER JOIN order_items ON products.id = order_items.product_id
WHERE products.supplier_id = 11
GROUP BY products.id, products.category
ORDER BY id DESC, min_quantity DESC
