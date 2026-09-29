SELECT s.supplier_name, s.country, p.supplier_id
FROM suppliers s
JOIN products p ON p.supplier_id = s.id
WHERE p.brand = 'Umbrella'
