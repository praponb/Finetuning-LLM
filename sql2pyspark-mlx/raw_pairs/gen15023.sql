SELECT dept_name, COUNT(*) AS total_count, COUNT(*) AS cnt
FROM departments
GROUP BY dept_name
HAVING SUM(budget) >= 19790000
ORDER BY total_count
