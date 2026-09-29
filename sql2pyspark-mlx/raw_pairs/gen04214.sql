WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS latest_rank
    FROM departments
    QUALIFY latest_rank = 1
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.dept_name,
        s.location,
        s.budget,
        x.job_title,
        x.salary
    FROM ranked s
    JOIN LATERAL (
            SELECT c.job_title, c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.hire_date BETWEEN '2025-01-01' AND '2025-04-01'
        )
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_name,
        budget,
        job_title,
        salary,
        LEAD(budget, 2) OVER (PARTITION BY job_title ORDER BY salary, id) AS next_budget,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY salary DESC, id) AS row_seq,
        SUM(budget) OVER (PARTITION BY dept_name) AS dept_name_sum_budget,
        SUM(salary) OVER (PARTITION BY dept_name ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_salary
    FROM not_flagged
)
SELECT
    id,
    dept_name,
    budget,
    job_title,
    next_budget,
    dept_name_sum_budget
FROM with_windows
ORDER BY dept_name DESC
