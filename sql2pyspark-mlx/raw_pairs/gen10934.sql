WITH not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM departments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.job_title IN ('Engineer', 'Analyst')
        )
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.dept_name,
        s.location,
        s.budget,
        top1.employee_job_title
    FROM not_flagged s
    LEFT JOIN LATERAL (
            SELECT c.job_title AS employee_job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(3) OVER w_ordered AS budget_bucket,
        ROW_NUMBER() OVER w_ordered AS seq_no,
        AVG(budget) OVER (PARTITION BY employee_job_title ORDER BY budget, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS budget_avg_6_prior,
        COUNT(budget) OVER w_grp AS employee_job_title_count_budget
    FROM with_latest
    WHERE dept_name = 'Engineering'
    WINDOW w_ordered AS (ORDER BY budget, id), w_grp AS (PARTITION BY employee_job_title)
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.employee_job_title_2,
        latest.employee_level
    FROM metrics s
    JOIN LATERAL (
            SELECT c.job_title AS employee_job_title_2, c.level AS employee_level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN budget >= 4028000 THEN 'LARGE' ELSE 'SMALL' END AS budget_tier,
    CASE WHEN employee_job_title_count_budget >= 15 THEN 'PREMIUM' ELSE 'STANDARD' END AS employee_job_title_count_budget_tier,
    COUNT(CASE WHEN location = 'New York' THEN 1 END) AS new_york_count,
    SIZE(COLLECT_SET(employee_job_title)) AS num_employee_job_titles,
    SIZE(COLLECT_SET(location)) AS num_locations,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN budget >= 4028000 THEN 'LARGE' ELSE 'SMALL' END ORDER BY COUNT(CASE WHEN location = 'New York' THEN 1 END) DESC) AS new_york_count_dense_rank
FROM with_last
GROUP BY budget_tier, employee_job_title_count_budget_tier
ORDER BY budget_tier DESC NULLS FIRST;
