WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN location = 'Tokyo' THEN 1 ELSE 0 END AS is_tokyo
    FROM departments
    WHERE dept_name IS NOT NULL
      AND location NOT IN ('Singapore', 'Bangkok')
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.location,
        s.is_tokyo,
        x.salary,
        x.job_title
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.salary, c.job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.salary > 75000
        )
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        is_tokyo,
        job_title,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY is_tokyo DESC, id) AS row_seq,
        SUM(is_tokyo) OVER w_part AS cumulative_is_tokyo,
        LAG(salary, 2) OVER (PARTITION BY location, job_title ORDER BY salary, id) AS prev_salary,
        SUM(salary) OVER w_part AS cumulative_salary,
        SUM(is_tokyo) OVER (PARTITION BY location, job_title ORDER BY is_tokyo, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_is_tokyo_2
    FROM not_flagged
    WINDOW w_part AS (PARTITION BY location ORDER BY is_tokyo, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    job_title,
    COUNT(*) FILTER (WHERE job_title = 'Intern') AS intern_rows,
    COUNT(DISTINCT id) AS distinct_ids
FROM with_windows
GROUP BY job_title
HAVING distinct_ids < 1000
ORDER BY job_title DESC NULLS FIRST;
