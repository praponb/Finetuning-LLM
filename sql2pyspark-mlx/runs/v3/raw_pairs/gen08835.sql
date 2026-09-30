SELECT id, city, ROUND(rating, 0) AS rating_rounded
FROM suppliers
WHERE city IN ('Singapore', 'Bangkok', 'Chiang Mai') AND (city = 'Berlin' OR city IN ('London', 'Tokyo'));
