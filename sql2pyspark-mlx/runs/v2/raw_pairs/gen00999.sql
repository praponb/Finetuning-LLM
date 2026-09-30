SELECT
    id,
    rating,
    city,
    country
FROM suppliers
WHERE rating = (SELECT MIN(rating) FROM suppliers)
ORDER BY rating DESC
