SELECT
    products.id,
    products.category,
    COUNT(order_items.id) AS order_item_count,
    MAX(order_items.unit_price) AS max_unit_price
FROM products
LEFT JOIN order_items ON products.id = order_items.product_id
WHERE order_items.unit_price BETWEEN 160 AND 200
GROUP BY products.id, products.category
ORDER BY category, order_item_count;
