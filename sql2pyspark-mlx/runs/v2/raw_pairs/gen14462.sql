SELECT s.id, s.supplier_name
FROM suppliers s
WHERE LENGTH(s.supplier_name) >= 6 AND EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.stock_qty BETWEEN 252 AND 344)
ORDER BY supplier_name;
