SELECT p.cost, oi.quantity
FROM products p
INNER JOIN order_items oi ON p.id = oi.product_id
WHERE p.product_name LIKE '%Shoe%'
ORDER BY oi.quantity
