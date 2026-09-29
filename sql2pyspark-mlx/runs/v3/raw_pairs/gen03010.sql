SELECT CASE WHEN dept_name IN ('Sales', 'Engineering') THEN 'group_a' ELSE 'other' END AS dept_name_group, SUM(budget) AS total_budget, COUNT(*) AS department_count
FROM departments
GROUP BY CASE WHEN dept_name IN ('Sales', 'Engineering') THEN 'group_a' ELSE 'other' END
ORDER BY dept_name_group ASC
