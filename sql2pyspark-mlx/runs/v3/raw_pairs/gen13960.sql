SELECT
    YEAR(hire_date) AS hire_year,
    MONTH(hire_date) AS hire_month,
    level,
    MIN(hire_date) AS first_hire_date
FROM employees
WHERE full_name LIKE '%FRAN%'
GROUP BY 1, 2, 3;
