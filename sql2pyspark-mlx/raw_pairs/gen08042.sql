SELECT
    CASE WHEN rating >= 3.6 THEN 'high' WHEN rating >= 2.5 THEN 'mid' ELSE 'low' END AS rating_bucket,
    CASE WHEN country IN ('TH', 'JP') THEN 'group_a' ELSE 'other' END AS country_group,
    ROUND(AVG(rating), 2) AS avg_rating,
    ROUND((100 * SUM(CASE WHEN city = 'London' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_london,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM suppliers
WHERE supplier_name IS NOT NULL
GROUP BY 1, 2
ORDER BY 1
