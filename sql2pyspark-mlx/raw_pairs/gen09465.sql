SELECT s.id, s.rating
FROM suppliers s
WHERE NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.price < 1050)
ORDER BY id
