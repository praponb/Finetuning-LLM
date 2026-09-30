SELECT location, COALESCE(budget, 0) AS budget_filled, CASE WHEN location IN ('New York', 'London') THEN 'Group A' ELSE 'Other' END AS location_group
FROM departments
WHERE budget > 2856000 AND location IN ('Tokyo', 'Bangkok') AND dept_name IN ('HR', 'Legal');
