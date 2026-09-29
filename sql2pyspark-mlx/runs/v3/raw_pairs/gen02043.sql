SELECT
    country,
    city,
    COUNT(*) total_count,
    SORT_ARRAY(COLLECT_SET(country)) country_list
FROM suppliers
WHERE rating >= 3.4 AND city IN ('Singapore', 'Berlin', 'Bangkok') AND rating BETWEEN 2.0 AND 2.5
GROUP BY country, city
