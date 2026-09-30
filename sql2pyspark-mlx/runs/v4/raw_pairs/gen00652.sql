SELECT CASE WHEN segment IN ('Corporate', 'Consumer') THEN 'core' ELSE 'other' END AS segment_group, MAX(age) - MIN(age) AS age_range
FROM customers
GROUP BY CASE WHEN segment IN ('Corporate', 'Consumer') THEN 'core' ELSE 'other' END
ORDER BY segment_group
LIMIT 5
