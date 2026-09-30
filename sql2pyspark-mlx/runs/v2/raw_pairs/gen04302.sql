SELECT
    s.id,
    s.supplier_name,
    s.city,
    s.country
FROM suppliers s
WHERE s.supplier_name LIKE '%Siam%' AND EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.stock_qty BETWEEN 224 AND 342)
