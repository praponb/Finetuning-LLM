SELECT s.id, s.rating FROM suppliers s WHERE s.rating >= 3.7 AND s.id IN (SELECT supplier_id FROM products)
