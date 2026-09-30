SELECT id, dept_id, LENGTH(level) AS level_length
FROM employees
WHERE (level = 'L3' AND UPPER(TRIM(full_name)) = 'IVY CHEN') OR LOWER(full_name) LIKE '%oah%'
ORDER BY id DESC, dept_id DESC;
