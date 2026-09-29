SELECT id, rating, city
FROM suppliers
WHERE rating <= (SELECT MIN(rating) FROM suppliers)
ORDER BY id
LIMIT 3;
