SELECT
    city,
    country,
    SUM(CASE WHEN country = 'JP' THEN 1 ELSE 0 END) AS jp_count,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    MAX(rating) AS max_rating
FROM suppliers
WHERE city IN ('Berlin', 'London', 'Bangkok') AND rating >= 2.4
GROUP BY city, country
HAVING max_rating < 1.9
