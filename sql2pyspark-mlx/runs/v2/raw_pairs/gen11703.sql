SELECT s.id, s.city, s.country, s.rating FROM suppliers s WHERE s.country = 'DE' AND s.id IN (SELECT supplier_id FROM products WHERE category != 'Sports')
