SELECT id, city, CAST(rating AS INT) AS rating_int
FROM suppliers
WHERE city = 'Bangkok'
