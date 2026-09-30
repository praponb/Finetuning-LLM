SELECT s.country, MIN(p.launch_date) AS first_launch_date FROM products p INNER JOIN suppliers s ON s.id = p.supplier_id GROUP BY s.country ORDER BY first_launch_date;
