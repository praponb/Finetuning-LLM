SELECT p.id, p.price
FROM products p
WHERE p.supplier_id IN (25, 34) AND p.supplier_id IN (SELECT id FROM suppliers WHERE rating > 2.7)
