SELECT id, supplier_name, FLOOR(rating) AS rating_whole
FROM suppliers
WHERE NOT (rating >= 3.0 AND rating BETWEEN 3.5 AND 3.6);
