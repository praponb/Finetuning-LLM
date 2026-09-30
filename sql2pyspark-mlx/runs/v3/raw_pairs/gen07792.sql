SELECT s.id, s.supplier_name
FROM suppliers s
WHERE s.rating BETWEEN 2.3 AND 3.5 AND EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.stock_qty = 366)
ORDER BY s.supplier_name, id
LIMIT 5
