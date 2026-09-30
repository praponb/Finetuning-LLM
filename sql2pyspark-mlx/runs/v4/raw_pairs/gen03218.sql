SELECT id, sqft, CASE WHEN region IN ('East', 'South') THEN 'Group A' ELSE 'Other' END AS region_group, ROUND(sqft, 1) AS sqft_rounded FROM stores;
