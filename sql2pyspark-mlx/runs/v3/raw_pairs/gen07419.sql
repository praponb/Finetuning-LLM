WITH valid_employees AS (
    SELECT *, CONCAT_WS('-', email, full_name) AS email_full_name_key
    FROM employees
    WHERE level NOT IN ('L5', 'L1')
),
Agg AS (
    SELECT level, MAX(salary) AS max_salary
    FROM valid_employees
    GROUP BY level
)
SELECT level, max_salary
FROM Agg
WHERE max_salary >= 61000
ORDER BY max_salary DESC;
