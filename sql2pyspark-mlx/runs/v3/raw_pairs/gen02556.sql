WITH valid_employees AS (
    SELECT *
    FROM employees
    WHERE level = 'L5'
),
hire_quarter_summary AS (
    SELECT
        QUARTER(hire_date) AS hire_quarter,
        is_remote,
        MIN(hire_date) AS min_hire_date,
        MIN(salary) AS min_salary
    FROM valid_employees
    GROUP BY QUARTER(hire_date), is_remote
)
SELECT
    hire_quarter,
    is_remote,
    min_hire_date,
    min_salary
FROM hire_quarter_summary
WHERE min_salary < 66000
ORDER BY min_salary DESC, hire_quarter, is_remote
LIMIT 50;
