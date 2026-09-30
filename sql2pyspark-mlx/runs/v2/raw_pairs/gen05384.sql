SELECT i.quantity, p.price, s.city
FROM inventory i
INNER JOIN products p ON p.id = i.product_id
INNER JOIN suppliers s ON p.supplier_id = s.id
