SELECT s.supplier_name, s.rating, s.city, p.launch_date FROM suppliers s LEFT JOIN products p ON p.supplier_id = s.id WHERE UPPER(s.supplier_name) LIKE '%LOB%' ORDER BY 3 DESC, rating DESC;
