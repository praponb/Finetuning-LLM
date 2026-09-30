SELECT id, rating, city
FROM suppliers
WHERE rating > (SELECT AVG(rating) FROM suppliers WHERE country IN ('SG', 'UK', 'JP'))
ORDER BY 3 ASC;
