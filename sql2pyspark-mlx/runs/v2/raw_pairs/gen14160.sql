SELECT
    id,
    full_name,
    CASE WHEN salary >= 123000 THEN 'High' WHEN salary >= 60000 THEN 'Medium' ELSE 'Low' END AS salary_level,
    DATE_ADD(hire_date, 51) AS hire_date_plus_51d
FROM employees;
