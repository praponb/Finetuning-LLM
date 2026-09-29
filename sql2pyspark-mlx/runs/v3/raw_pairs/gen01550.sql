SELECT
    location,
    CASE WHEN budget >= 3714000 THEN 'high' WHEN budget >= 891000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    SUM(CASE WHEN dept_name = 'Legal' THEN budget ELSE 0 END) AS legal_budget,
    SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list
FROM departments
GROUP BY location, CASE WHEN budget >= 3714000 THEN 'high' WHEN budget >= 891000 THEN 'mid' ELSE 'low' END
