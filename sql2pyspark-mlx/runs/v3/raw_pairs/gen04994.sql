SELECT * FROM (SELECT CASE WHEN country IN ('SG', 'US') THEN 'group_a' ELSE 'other' END AS country_group, city, rating FROM suppliers)
PIVOT (MAX(rating) FOR city IN ('Bangkok', 'Tokyo'))
ORDER BY country_group
