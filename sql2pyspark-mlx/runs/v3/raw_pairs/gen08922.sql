SELECT
    CASE WHEN country IN ('JP', 'TH') THEN 'priority' ELSE 'other' END country_group,
    city,
    COUNT(rating) rating_count,
    ROUND(AVG(rating), 2) avg_rating
FROM suppliers
WHERE rating < 3.7
GROUP BY CASE WHEN country IN ('JP', 'TH') THEN 'priority' ELSE 'other' END, city
ORDER BY 3 DESC, country_group
