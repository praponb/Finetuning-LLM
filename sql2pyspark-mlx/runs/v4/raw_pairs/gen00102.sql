SELECT manager_id
FROM employees
EXCEPT ALL
SELECT manager_id
FROM employees
WHERE LENGTH(full_name) > 6;
