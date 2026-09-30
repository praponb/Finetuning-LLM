WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        UPPER(location) AS location_upper,
        CASE WHEN budget >= 3443000 THEN 'High' WHEN budget >= 1646000 THEN 'Medium' ELSE 'Low' END AS budget_band
    FROM departments
    WHERE dept_name IS NOT NULL
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.employee_job_title,
        x.employee_level
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.job_title AS employee_job_title, c.level AS employee_level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        location,
        budget,
        location_upper,
        employee_job_title,
        employee_level,
        LAG(budget) OVER w_grp AS prev_budget,
        ROW_NUMBER() OVER w_grp AS row_seq
    FROM latest_match
    WINDOW w_grp AS (PARTITION BY dept_name, employee_job_title ORDER BY budget, id)
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.location,
        s.budget,
        s.location_upper,
        s.employee_level,
        top1.job_title,
        top1.employee_hire_date
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.job_title, c.hire_date AS employee_hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        budget,
        employee_level,
        job_title,
        employee_hire_date,
        LEAD(budget, 1) OVER (PARTITION BY employee_level, location ORDER BY budget, id) AS next_budget,
        ROUND(budget / SUM(budget) OVER (PARTITION BY location, employee_level), 4) AS budget_share,
        AVG(budget) OVER (PARTITION BY DATE_TRUNC('MONTH', employee_hire_date), location ORDER BY employee_hire_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS budget_avg_3,
        DENSE_RANK() OVER (PARTITION BY MONTH(employee_hire_date) ORDER BY budget DESC NULLS FIRST) AS budget_dense_rank
    FROM with_last
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.hire_date < s.employee_hire_date
        )
),
kept_departments AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.hire_date >= s.employee_hire_date
        )
)
SELECT
    id,
    budget,
    employee_level,
    employee_hire_date,
    next_budget,
    budget_dense_rank,
    CASE WHEN employee_level RLIKE '^(L2|L4)$' THEN 'PRIORITY' WHEN employee_level RLIKE '^(L1|L3)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS employee_level_zone
FROM kept_departments
ORDER BY budget_dense_rank DESC NULLS LAST;
