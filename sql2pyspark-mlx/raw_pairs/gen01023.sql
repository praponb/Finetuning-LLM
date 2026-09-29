SELECT status
FROM subscriptions
EXCEPT
SELECT dept_name
FROM departments
WHERE dept_name = 'Legal'
