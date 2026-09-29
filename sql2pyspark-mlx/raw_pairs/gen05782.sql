SELECT
    CASE WHEN YEAR(hire_date) < 2024 THEN 'before_2024' WHEN YEAR(hire_date) = 2024 THEN '2024' ELSE 'after_2024' END AS hire_cohort,
    QUARTER(hire_date) AS hire_quarter,
    COUNT(*) AS total_count,
    COUNT(*) AS employee_count,
    COUNT(*) AS cnt
FROM employees
WHERE hire_date < '2025-01-01' AND dept_id IN (9, 18, 27)
GROUP BY hire_cohort, hire_quarter;
