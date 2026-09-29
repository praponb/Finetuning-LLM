SELECT *
FROM (
    SELECT city, ROUND(STDDEV(rating), 2) stddev_rating, SUM(CASE WHEN country = 'UK' THEN 1 ELSE 0 END) uk_count
    FROM suppliers
    GROUP BY city
) x
WHERE stddev_rating < 76
