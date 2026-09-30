SELECT dept_name, COUNT(CASE WHEN dept_name = 'Legal' THEN 1 END) AS legal_count, COUNT(DISTINCT budget) AS unique_dept_names
FROM departments
WHERE (budget BETWEEN 1564000 AND 2615000 AND location IN ('London', 'New York', 'Chiang Mai')) OR budget BETWEEN 1618000 AND 1880000
GROUP BY dept_name
ORDER BY 1 DESC
