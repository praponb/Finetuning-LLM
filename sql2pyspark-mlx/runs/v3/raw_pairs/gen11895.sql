SELECT p.launch_date, s.country
FROM products p
INNER JOIN suppliers s ON p.supplier_id = s.id
ORDER BY 1, country;
