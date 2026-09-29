SELECT s.id, s.rating, s.city FROM suppliers s WHERE s.supplier_name LIKE '%Globa%' AND s.id IN (SELECT supplier_id FROM products WHERE SIZE(tags) > 2) ORDER BY s.rating;
