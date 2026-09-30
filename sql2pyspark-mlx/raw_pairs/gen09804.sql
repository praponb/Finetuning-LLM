WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary, id) AS rank_in_group
    FROM employees
    QUALIFY rank_in_group = 1
)
SELECT
    s.id,
    s.full_name,
    s.dept_id,
    s.job_title,
    s.salary,
    s.hire_date,
    s.is_remote,
    s.level,
    (SELECT SUM(t2.salary) FROM employees t2 WHERE t2.dept_id = s.dept_id) AS sum_employee_salary
FROM latest s;
