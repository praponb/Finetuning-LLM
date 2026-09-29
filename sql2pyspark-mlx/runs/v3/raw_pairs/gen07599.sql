SELECT COUNT(DISTINCT budget) AS unique_dept_names
FROM departments
WHERE budget >= 3784000 AND budget <= 1204000
ORDER BY 1 DESC;
