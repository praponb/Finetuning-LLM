SELECT city, stddev_rating
FROM (
    SELECT city, ROUND(STDDEV(rating), 2) AS stddev_rating
    FROM suppliers
    WHERE rating <= 2.4
    GROUP BY city
) AS t
WHERE stddev_rating < 71
ORDER BY stddev_rating DESC
