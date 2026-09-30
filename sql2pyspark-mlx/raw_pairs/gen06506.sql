SELECT ROUND(AVG(budget), 2) AS avg_budget, SUM(CASE WHEN location = 'London' THEN budget ELSE 0 END) AS london_budget
FROM departments
ORDER BY avg_budget DESC;
