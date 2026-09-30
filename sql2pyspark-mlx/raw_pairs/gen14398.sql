SELECT CASE WHEN region IN ('East', 'South') THEN 'group_a' ELSE 'other' END AS region_group, ROUND(AVG(sqft), 2) AS avg_sqft FROM stores GROUP BY 1;
