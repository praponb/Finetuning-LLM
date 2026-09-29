SELECT s.id, s.supplier_name
FROM suppliers s
WHERE s.rating <= 2.3 AND s.id IN (SELECT supplier_id FROM products WHERE category = 'Books')
