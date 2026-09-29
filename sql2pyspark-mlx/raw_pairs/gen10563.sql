WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(dept_name, 1, 3) AS dept_name_prefix,
        SUBSTRING(location, 1, 4) AS location_prefix,
        COALESCE(dept_name, 'N/A') AS resolved_dept_name
    FROM departments
    WHERE dept_name IS NOT NULL
      AND location IN ('London', 'Tokyo', 'Bangkok')
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(budget) OVER w_ordered AS running_count_budget,
        AVG(budget) OVER w_ordered AS running_avg_budget,
        MAX(budget) OVER w_ordered AS running_max_budget,
        MIN(budget) OVER w_ordered AS running_min_budget,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS seq_no
    FROM valid
    WINDOW w_ordered AS (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.employee_hire_date,
        latest.salary
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.hire_date AS employee_hire_date, c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.location,
        s.dept_name_prefix,
        s.resolved_dept_name,
        s.running_count_budget,
        s.running_avg_budget,
        s.running_max_budget,
        s.running_min_budget,
        s.seq_no,
        s.employee_hire_date,
        x.job_title,
        x.employee_salary
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.job_title, c.salary AS employee_salary
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.hire_date <= s.employee_hire_date
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Aggregate with conditional metrics
SELECT
    job_title,
    SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list,
    MIN(running_count_budget) AS min_running_count_budget,
    PERCENTILE_APPROX(seq_no, 0.95) AS seq_no_p95
FROM latest_match
GROUP BY job_title
ORDER BY min_running_count_budget DESC NULLS FIRST;
