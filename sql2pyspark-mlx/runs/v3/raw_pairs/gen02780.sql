SELECT CASE WHEN region IN ('East', 'South') THEN 'group_a' ELSE 'other' END AS region_group, MAX(sqft) - MIN(sqft) AS sqft_range
FROM stores
GROUP BY CASE WHEN region IN ('East', 'South') THEN 'group_a' ELSE 'other' END;
