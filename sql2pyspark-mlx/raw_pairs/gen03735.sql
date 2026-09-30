SELECT s.city, COUNT(p.id) product_count FROM suppliers s LEFT OUTER JOIN products p ON p.supplier_id = s.id WHERE s.country = 'UK' GROUP BY s.city ORDER BY s.city LIMIT 3;
