WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, SUBSTRING(dept_name, 1, 2) AS dept_name_prefix
    FROM departments
    WHERE budget IS NULL
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.dept_id IN (35, 36)
        )
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(budget) OVER (PARTITION BY location, dept_name ORDER BY budget, id) AS next_budget,
        ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY budget, id) AS event_seq
    FROM eligible
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_name,
        budget,
        dept_name_prefix,
        next_budget,
        LEAD(budget) OVER (PARTITION BY location ORDER BY next_budget, id) AS next_budget_2,
        LEAD(event_seq) OVER (PARTITION BY location ORDER BY next_budget, id) AS next_event_seq,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY next_budget, id) AS event_seq_2,
        ROUND(event_seq / SUM(event_seq) OVER (PARTITION BY dept_name, location), 4) AS event_seq_share
    FROM with_windows
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND YEAR(x.hire_date) = 2025
        )
),
with_windows_departments AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(next_budget_2) OVER (PARTITION BY dept_name ORDER BY event_seq_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_next_budget_2,
        SUM(next_budget_2) OVER (PARTITION BY dept_name ORDER BY event_seq_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_next_budget_2,
        COUNT(next_budget_2) OVER (PARTITION BY dept_name ORDER BY event_seq_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_next_budget_2,
        NTILE(10) OVER (ORDER BY budget DESC, id) AS budget_bucket
    FROM not_flagged
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.budget,
        s.next_budget,
        s.next_budget_2,
        s.next_event_seq,
        s.event_seq_share,
        s.running_min_next_budget_2,
        s.running_sum_next_budget_2,
        s.running_count_next_budget_2,
        s.budget_bucket,
        latest.employee_job_title
    FROM with_windows_departments s
    JOIN LATERAL (
            SELECT c.job_title AS employee_job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    next_budget,
    next_budget_2,
    next_event_seq,
    event_seq_share,
    running_min_next_budget_2,
    running_sum_next_budget_2,
    running_count_next_budget_2,
    employee_job_title,
    CASE WHEN employee_job_title RLIKE '^(Manager|Analyst)$' THEN 'PRIORITY' WHEN employee_job_title RLIKE '^(Engineer|Director)$' THEN 'CORE' ELSE 'GROUP_B' END AS employee_job_title_zone
FROM latest_match
ORDER BY running_sum_next_budget_2
