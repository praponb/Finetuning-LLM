SELECT dept_name, CASE WHEN location IN ('Chiang Mai', 'Tokyo') THEN 'Priority' ELSE 'Other' END AS location_group FROM departments ORDER BY location_group
