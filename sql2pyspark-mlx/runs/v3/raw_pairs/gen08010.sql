SELECT *
FROM (
    SELECT CASE WHEN country IN ('SG', 'DE') THEN 'core' ELSE 'other' END country_group, SORT_ARRAY(COLLECT_SET(country)) country_list, MIN(rating) min_rating
    FROM suppliers
    WHERE rating < 2.7
    GROUP BY CASE WHEN country IN ('SG', 'DE') THEN 'core' ELSE 'other' END
) sub
WHERE min_rating > 1.9
ORDER BY 1
LIMIT 5;
