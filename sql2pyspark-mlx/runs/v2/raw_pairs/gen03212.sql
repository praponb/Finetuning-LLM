SELECT p.brand, s.supplier_name, s.country
FROM products p
JOIN suppliers s ON p.supplier_id = s.id
ORDER BY country, supplier_name;
