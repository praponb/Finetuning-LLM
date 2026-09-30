SELECT s.id, s.supplier_name
FROM suppliers s
WHERE s.supplier_name LIKE '%Imp%' AND EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.stock_qty BETWEEN 102 AND 113)
ORDER BY supplier_name, id DESC
