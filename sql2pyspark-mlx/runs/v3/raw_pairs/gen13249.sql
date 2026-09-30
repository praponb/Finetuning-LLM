SELECT rating, CAST(rating AS INT) AS rating_int, COALESCE(city, 'Unknown') AS city_filled
FROM suppliers
WHERE city = 'Singapore' AND city = 'London';
