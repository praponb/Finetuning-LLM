SELECT *, CASE WHEN region IN ('South', 'North') THEN 'Group A' ELSE 'Other' END AS region_group
FROM stores
WHERE store_name LIKE 'Ma%' AND store_name LIKE '%Airp%'
ORDER BY city;
