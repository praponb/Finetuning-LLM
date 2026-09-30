SELECT country_group, avg_rating, rating_range
FROM (
    SELECT CASE WHEN country IN ('JP', 'DE') THEN 'core' ELSE 'other' END AS country_group, AVG(rating) AS avg_rating, MAX(rating) - MIN(rating) AS rating_range
    FROM suppliers
    GROUP BY CASE WHEN country IN ('JP', 'DE') THEN 'core' ELSE 'other' END
) AS t
WHERE rating_range < 70
ORDER BY country_group ASC;
