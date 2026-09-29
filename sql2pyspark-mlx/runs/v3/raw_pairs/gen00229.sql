SELECT i.product_id, p.product_name, s.city
FROM inventory i
RIGHT JOIN products p ON i.product_id = p.id
JOIN suppliers s ON s.id = p.supplier_id
