SELECT location_group, pct_singapore
FROM (
    SELECT CASE WHEN location IN ('Tokyo', 'New York') THEN 'core' ELSE 'other' END AS location_group, ROUND((100 * SUM(CASE WHEN location = 'Singapore' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_singapore
    FROM departments
    GROUP BY CASE WHEN location IN ('Tokyo', 'New York') THEN 'core' ELSE 'other' END
) AS t
WHERE pct_singapore >= 63;
