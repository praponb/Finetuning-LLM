SELECT oi.id, oi.quantity, p.launch_date
FROM order_items oi
INNER JOIN products p ON oi.product_id = p.id
WHERE oi.quantity = 4 AND p.supplier_id IS NULL;
