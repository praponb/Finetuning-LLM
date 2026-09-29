SELECT CASE WHEN region IN ('Central', 'North') THEN 'group_a' ELSE 'other' END AS region_group, ROUND(STDDEV(sqft), 2) AS stddev_sqft, MAX(sqft) AS max_sqft
FROM stores
WHERE sqft BETWEEN 10823 AND 13652 OR opened_date >= '2024-02-22'
GROUP BY region_group
ORDER BY max_sqft, stddev_sqft DESC;
