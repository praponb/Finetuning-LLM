SELECT p.id, p.category, p.launch_date, s.supplier_name, s.country, s.rating FROM products p FULL OUTER JOIN suppliers s ON s.id = p.supplier_id WHERE p.price <= 1050 ORDER BY country
