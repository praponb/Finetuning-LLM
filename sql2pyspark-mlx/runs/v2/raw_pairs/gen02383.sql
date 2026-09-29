SELECT s.id, s.rating, s.country FROM suppliers s WHERE s.id IN (SELECT supplier_id FROM products WHERE category = 'Grocery')
