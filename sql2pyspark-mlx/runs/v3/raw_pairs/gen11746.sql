SELECT s.id, s.city, COUNT(p.id) product_count FROM suppliers s LEFT JOIN products p ON s.id = p.supplier_id WHERE s.rating <= 3.6 GROUP BY s.id, s.city;
