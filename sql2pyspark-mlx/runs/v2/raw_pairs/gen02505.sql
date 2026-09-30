SELECT p.category, COUNT(oi.id) AS order_item_count
FROM products p
INNER JOIN order_items oi ON oi.product_id = p.id
WHERE p.category IN ('Grocery', 'Sports')
GROUP BY p.category;
