SELECT s.supplier_name, s.rating, p.launch_date
FROM suppliers s
INNER JOIN products p ON p.supplier_id = s.id
ORDER BY 1
