SELECT *
FROM (
    SELECT country, ROUND(AVG(rating), 2) avg_rating, SUM(CASE WHEN country = 'TH' THEN rating ELSE 0 END) th_rating
    FROM suppliers
    WHERE country = 'US'
    GROUP BY country
) t
WHERE avg_rating > 27
ORDER BY country, avg_rating DESC;
