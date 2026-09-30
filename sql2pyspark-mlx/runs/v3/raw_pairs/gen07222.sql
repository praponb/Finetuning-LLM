SELECT id, rating, city
FROM suppliers
WHERE rating >= (SELECT AVG(rating) FROM suppliers WHERE supplier_name IS NOT NULL)
ORDER BY city DESC
