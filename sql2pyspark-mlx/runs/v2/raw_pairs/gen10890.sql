SELECT s.id, COUNT(p.id) product_count, MIN(p.launch_date) first_launch_date, MAX(p.price) max_price FROM suppliers s LEFT JOIN products p ON s.id = p.supplier_id GROUP BY s.id
