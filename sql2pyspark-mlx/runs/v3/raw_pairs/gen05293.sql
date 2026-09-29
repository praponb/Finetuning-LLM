SELECT
    id,
    rating,
    city,
    supplier_name
FROM suppliers
WHERE rating >= (SELECT AVG(rating) FROM suppliers WHERE city != 'Berlin')
ORDER BY rating DESC
