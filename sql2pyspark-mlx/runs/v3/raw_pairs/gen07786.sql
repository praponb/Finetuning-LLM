SELECT CASE WHEN dept_name IN ('HR', 'Legal') THEN 'tier_1' ELSE 'other' END AS dept_name_group, ROUND(AVG(budget), 2) AS avg_budget FROM departments GROUP BY 1
