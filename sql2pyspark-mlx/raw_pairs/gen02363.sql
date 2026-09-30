WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(dept_name) AS dept_name_length,
        COALESCE(dept_name, 'N/A') AS dept_name_clean
    FROM departments
    WHERE dept_name IS NOT NULL
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.dept_name,
        s.location,
        s.dept_name_clean,
        x.employee_level,
        x.salary
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.level AS employee_level, c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.dept_name,
        s.salary,
        top1.job_title,
        top1.level
    FROM latest_match s
    JOIN LATERAL (
            SELECT c.job_title, c.level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        dept_name,
        salary,
        job_title,
        level,
        CONCAT_WS(',', level, dept_name) AS level_dept_name_key,
        COALESCE(job_title, 'N/A') AS job_title_filled
    FROM with_last
    WHERE level = 'L1'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(salary) OVER (PARTITION BY level, dept_name ORDER BY salary, id) AS next_salary,
        ROW_NUMBER() OVER (PARTITION BY level, dept_name ORDER BY salary, id) AS seq_no,
        ROUND(salary / SUM(salary) OVER (PARTITION BY dept_name), 4) AS salary_share
    FROM valid
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.level IN ('L4', 'L2', 'L3')
        )
)
-- Grouped metrics
SELECT
    CASE WHEN salary_share >= 0.86 THEN 'LARGE' ELSE 'SMALL' END AS salary_share_tier,
    ROUND(SUM(seq_no) / NULLIF(COUNT(CASE WHEN level = 'L5' THEN 1 END), 0), 2) AS seq_no_per_l5,
    PERCENTILE_APPROX(seq_no, 0.5) AS seq_no_p50,
    SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list
FROM kept
GROUP BY CASE WHEN salary_share >= 0.86 THEN 'LARGE' ELSE 'SMALL' END
HAVING PERCENTILE_APPROX(seq_no, 0.5) > 0
