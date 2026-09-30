SELECT hire_date event_date, 'employee' source
FROM employees
WHERE job_title != 'Designer'
UNION ALL
SELECT opened_date event_date, 'account' source
FROM accounts
WHERE status = 'CLOSED'
ORDER BY event_date DESC
