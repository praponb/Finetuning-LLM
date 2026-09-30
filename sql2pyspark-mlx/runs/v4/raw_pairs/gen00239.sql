SELECT city, MIN(rating) AS min_rating
FROM suppliers
WHERE country = 'US' AND (rating >= 4.2 OR city NOT IN ('Chiang Mai', 'Tokyo'))
GROUP BY city
ORDER BY min_rating;
