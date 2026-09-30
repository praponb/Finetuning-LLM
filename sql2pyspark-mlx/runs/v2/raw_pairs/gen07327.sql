SELECT id, rating, supplier_name
FROM suppliers
WHERE rating = (SELECT MIN(rating) FROM suppliers);
