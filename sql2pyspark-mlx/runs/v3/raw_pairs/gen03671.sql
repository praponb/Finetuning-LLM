SELECT *
FROM (
    SELECT CASE WHEN segment IN ('Consumer', 'SMB') THEN 'core' ELSE 'other' END AS segment_group, COUNT(DISTINCT credit_limit) AS unique_countrys, COUNT(CASE WHEN segment = 'Consumer' THEN 1 END) AS consumer_count
    FROM customers
    WHERE name LIKE '%Lee%'
    GROUP BY CASE WHEN segment IN ('Consumer', 'SMB') THEN 'core' ELSE 'other' END
) AS t
WHERE consumer_count >= 4
ORDER BY segment_group
LIMIT 3;
