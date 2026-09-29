SELECT p.id, p.price
FROM products p
WHERE p.stock_qty <= 248 AND p.supplier_id NOT IN (SELECT id FROM suppliers WHERE country IN ('SG', 'JP'))
