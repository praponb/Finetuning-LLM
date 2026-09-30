SELECT s.id, s.rating FROM suppliers s WHERE s.rating IS NOT NULL AND s.id IN (SELECT supplier_id FROM products WHERE SIZE(tags) >= 2) ORDER BY id, rating
