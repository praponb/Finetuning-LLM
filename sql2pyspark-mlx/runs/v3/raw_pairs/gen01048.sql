SELECT AVG(rating) AS avg_rating, MAX(rating) AS max_rating
FROM suppliers
ORDER BY max_rating ASC;
