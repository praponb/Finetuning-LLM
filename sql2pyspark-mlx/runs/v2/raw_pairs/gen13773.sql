SELECT
    city,
    SUM(CASE WHEN country = 'SG' THEN rating ELSE 0 END) sg_rating,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) country_values,
    COUNT(CASE WHEN country = 'SG' THEN 1 END) sg_count
FROM suppliers
GROUP BY city
HAVING sg_count >= 5
