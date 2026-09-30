SELECT p.supplier_id, p.category, SUM(oi.quantity) total_quantity
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
GROUP BY p.supplier_id, p.category
HAVING SUM(oi.quantity) >= 15
