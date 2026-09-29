SELECT
    CASE WHEN d.budget >= 3554000 THEN 'high' WHEN d.budget >= 1516000 THEN 'mid' ELSE 'low' END AS budget_bucket,
    CASE WHEN d.location IN ('Bangkok', 'Chiang Mai') THEN 'tier_1' ELSE 'other' END AS location_group,
    MAX(e.salary) - MIN(e.salary) AS salary_range,
    MIN(e.hire_date) AS min_hire_date
FROM employees e
INNER JOIN departments d ON d.id = e.dept_id
WHERE e.dept_id IN (16, 27)
GROUP BY CASE WHEN d.budget >= 3554000 THEN 'high' WHEN d.budget >= 1516000 THEN 'mid' ELSE 'low' END, CASE WHEN d.location IN ('Bangkok', 'Chiang Mai') THEN 'tier_1' ELSE 'other' END
