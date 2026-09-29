SELECT id, rating, country
FROM suppliers
WHERE rating >= (SELECT MAX(rating) FROM suppliers)
ORDER BY rating, id DESC
LIMIT 20
