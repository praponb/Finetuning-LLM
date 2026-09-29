SELECT p.id, p.supplier_id
FROM products p
WHERE p.category IN ('Books', 'Sports', 'Fashion') AND p.supplier_id NOT IN (SELECT id FROM suppliers WHERE rating < 3.8)
