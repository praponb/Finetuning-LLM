WITH valid AS (
    SELECT
        id,
        email,
        dept_id,
        manager_id,
        salary,
        hire_date,
        is_remote,
        SPLIT(LOWER(email), '@')[0] AS email_user,
        CAST(salary AS INT) AS salary_int
    FROM employees
    WHERE hire_date >= DATE '2023-08-05'
      AND dept_id = 12
)
SELECT
    id,
    email,
    email_user,
    salary_int
FROM valid
QUALIFY ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY salary_int DESC, id) = 1
