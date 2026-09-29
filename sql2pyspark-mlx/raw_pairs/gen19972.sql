SELECT oi.quantity, p.cost, s.country
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.id
LEFT JOIN suppliers s ON s.id = p.supplier_id;
