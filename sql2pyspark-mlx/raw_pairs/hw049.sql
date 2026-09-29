SELECT id, dept_id, salary,
       LAST_VALUE(salary) OVER (PARTITION BY dept_id ORDER BY salary
                                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS highest_salary
FROM employees;
