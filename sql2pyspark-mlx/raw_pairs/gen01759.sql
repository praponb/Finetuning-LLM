SELECT country_group, unique_countrys
FROM (
    SELECT CASE WHEN country IN ('DE', 'SG') THEN 'core' ELSE 'other' END AS country_group, COUNT(DISTINCT rating) AS unique_countrys
    FROM suppliers
    GROUP BY CASE WHEN country IN ('DE', 'SG') THEN 'core' ELSE 'other' END
) AS agg
WHERE unique_countrys >= 7
