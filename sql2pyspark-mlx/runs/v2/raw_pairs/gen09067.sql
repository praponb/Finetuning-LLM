SELECT s.id, s.country, s.rating
FROM suppliers s
WHERE s.rating >= 3.6 AND EXISTS (SELECT 1 FROM products p WHERE p.supplier_id = s.id AND p.product_name LIKE '%Nois%')
