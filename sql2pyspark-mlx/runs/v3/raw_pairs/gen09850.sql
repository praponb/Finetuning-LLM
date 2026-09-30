SELECT budget, dept_name, CASE WHEN location IN ('Chiang Mai', 'Berlin') THEN 'Group A' ELSE 'Other' END AS location_group, ROUND(budget * 1.15, 2) AS adjusted_budget FROM departments
