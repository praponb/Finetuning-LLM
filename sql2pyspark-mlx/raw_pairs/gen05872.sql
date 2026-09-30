SELECT
    city,
    country,
    ROUND(STDDEV(rating), 2) AS stddev_rating,
    SUM(rating) AS total_rating
FROM suppliers
WHERE rating < 3.0 AND city = 'Singapore' AND rating > 4.1
GROUP BY CUBE(city, country);
