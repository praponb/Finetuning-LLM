SELECT s.id, s.city, COUNT(p.id) product_count FROM suppliers s LEFT OUTER JOIN products p ON s.id = p.supplier_id WHERE p.price < 950 GROUP BY s.id, s.city ORDER BY product_count DESC
