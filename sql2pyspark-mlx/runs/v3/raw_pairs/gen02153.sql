SELECT products.id, products.brand, COUNT(order_items.id) AS order_item_count
FROM products
LEFT JOIN order_items ON order_items.product_id = products.id
GROUP BY products.id, products.brand
HAVING COUNT(order_items.id) > 1;
