SELECT id, country, ROUND(rating, 1) AS rating_rounded
FROM suppliers
ORDER BY id DESC
