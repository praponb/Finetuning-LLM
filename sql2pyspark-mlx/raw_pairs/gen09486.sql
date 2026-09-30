WITH kept AS (
    SELECT s.*
    FROM departments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.job_title NOT IN ('Designer', 'Manager', 'Director')
        )
),
kept_departments AS (
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.level IN ('L2', 'L1', 'L5')
        )
),
with_latest AS (
    SELECT
        s.id,
        s.budget,
        top1.job_title
    FROM kept_departments s
    LEFT JOIN LATERAL (
            SELECT c.job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
valid AS (
    SELECT
        *,
        UPPER(job_title) AS job_title_upper,
        COALESCE(job_title, 'DIRECT') AS job_title_filled
    FROM with_latest
    WHERE job_title IS NOT NULL
)
SELECT
    id,
    budget,
    job_title,
    NTILE(3) OVER (PARTITION BY job_title ORDER BY budget, id) AS budget_bucket,
    LEAD(budget, 1) OVER (PARTITION BY job_title ORDER BY budget, id) AS next_budget,
    PERCENT_RANK() OVER (PARTITION BY job_title ORDER BY budget DESC NULLS LAST) AS budget_percent_rank,
    COUNT(budget) OVER (PARTITION BY job_title) AS job_title_count_budget
FROM valid
WHERE job_title <> 'Manager';
