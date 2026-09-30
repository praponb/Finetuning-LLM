SELECT * FROM (SELECT CASE WHEN YEAR(hire_date) < 2024 THEN 'before_2024' WHEN YEAR(hire_date) = 2024 THEN '2024' ELSE 'after_2024' END AS hire_cohort, level, salary FROM employees)
PIVOT (AVG(salary) FOR level IN ('L2', 'L1'))
ORDER BY hire_cohort
