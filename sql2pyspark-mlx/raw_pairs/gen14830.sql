SELECT
    city,
    city_group,
    rating_range,
    avg_rating
FROM (
    SELECT
        city,
        CASE WHEN city IN ('Bangkok', 'Berlin') THEN 'group_a' ELSE 'other' END AS city_group,
        MAX(rating) - MIN(rating) AS rating_range,
        ROUND(AVG(rating), 2) AS avg_rating
    FROM suppliers
    GROUP BY city, CASE WHEN city IN ('Bangkok', 'Berlin') THEN 'group_a' ELSE 'other' END
) AS t
WHERE avg_rating < 26;
