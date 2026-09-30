SELECT p.id, p.stock_qty, p.supplier_id
FROM products p
WHERE p.supplier_id NOT IN (SELECT id FROM suppliers WHERE country = 'DE')
ORDER BY p.stock_qty;
