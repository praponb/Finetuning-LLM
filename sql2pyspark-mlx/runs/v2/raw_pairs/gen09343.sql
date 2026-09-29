SELECT
    city,
    country,
    ROUND(AVG(rating), 2) AS avg_rating,
    MAX(rating) AS max_rating
FROM suppliers
GROUP BY ROLLUP(city, country);
