SELECT
    s.id,
    s.rating,
    s.supplier_name,
    s.city
FROM suppliers s
WHERE NOT EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.product_name LIKE 'N%');
