SELECT id, rating, supplier_name
FROM suppliers
WHERE rating > (SELECT MAX(rating) FROM suppliers WHERE supplier_name LIKE 'Pac%')
ORDER BY id DESC;
