SELECT device AS label
FROM sessions
UNION ALL
SELECT email AS label
FROM employees
ORDER BY label
