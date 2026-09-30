SELECT p.id, p.supplier_id, p.product_name
FROM products p
WHERE p.stock_qty BETWEEN 206 AND 260 AND p.supplier_id NOT IN (SELECT id FROM suppliers WHERE supplier_name LIKE 'P%')
ORDER BY supplier_id, id
LIMIT 5
