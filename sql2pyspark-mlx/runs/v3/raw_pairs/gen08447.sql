SELECT s.country, s.rating, s.city, p.price, p.category FROM suppliers s LEFT JOIN products p ON s.id = p.supplier_id WHERE p.launch_date BETWEEN '2023-07-01' AND '2023-10-01';
