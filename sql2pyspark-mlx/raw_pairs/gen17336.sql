SELECT device
FROM sessions
WHERE browser IN ('Firefox', 'Edge', 'Chrome')
UNION
SELECT level AS device
FROM employees
