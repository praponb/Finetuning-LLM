SELECT id, country, COALESCE(rating, 0) AS rating_filled
FROM suppliers
WHERE rating <= 2.3
ORDER BY country
