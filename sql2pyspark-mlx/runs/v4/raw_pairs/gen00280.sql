SELECT country, SUM(rating) AS total_rating
FROM suppliers
WHERE country = 'UK' AND LOWER(TRIM(supplier_name)) = 'tokyo traders'
GROUP BY country
HAVING total_rating > 22
ORDER BY total_rating;
