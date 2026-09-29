SELECT s.supplier_name, s.country, p.price
FROM suppliers s
LEFT JOIN products p ON p.supplier_id = s.id
ORDER BY supplier_name;
