SELECT p.stock_qty, i.product_id, i.quantity
FROM products p
LEFT JOIN inventory i ON i.product_id = p.id
WHERE MONTH(i.last_restock) IN (1, 10, 12) OR i.product_id IN (5, 9, 10)
ORDER BY product_id DESC
