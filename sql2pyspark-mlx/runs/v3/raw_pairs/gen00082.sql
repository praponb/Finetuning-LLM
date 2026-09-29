SELECT p.brand, s.rating
FROM products p
LEFT OUTER JOIN suppliers s ON p.supplier_id = s.id
ORDER BY p.brand
