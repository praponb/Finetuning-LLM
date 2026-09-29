SELECT p.supplier_id, s.supplier_name
FROM products p
JOIN suppliers s ON s.id = p.supplier_id
WHERE p.stock_qty >= 297
