WITH valid_employees AS (
    SELECT *, CONCAT_WS('-', email, full_name) AS email_full_name_key
    FROM employees
    WHERE level NOT IN ('L5', 'L1')
),
Agg AS (
    SELECT manager_id, MAX(salary) AS max_salary
    FROM valid_employees
    GROUP BY manager_id
)
SELECT manager_id, max_salary
FROM Agg
WHERE max_salary >= 61000
ORDER BY max_salary DESC;
