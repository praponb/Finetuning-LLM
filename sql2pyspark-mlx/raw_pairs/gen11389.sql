WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        UPPER(full_name) AS full_name_upper
    FROM employees
    WHERE is_remote = TRUE
),
with_dims AS (
    -- Attach departments attributes
    SELECT
        src.*,
        d.location,
        d.budget
    FROM filtered src
    INNER JOIN departments d
        ON src.dept_id = d.id
),
with_dims_employees AS (
    -- Attach departments attributes
    SELECT
        e.*,
        d.dept_name,
        d.budget AS department_budget,
        d.location AS department_location
    FROM with_dims e
    LEFT JOIN departments d
        ON e.dept_id = d.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        email,
        dept_id,
        manager_id,
        full_name_upper,
        location,
        dept_name,
        department_budget,
        PERCENT_RANK() OVER w_running AS department_budget_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY manager_id, department_location ORDER BY department_budget, id) AS row_seq
    FROM with_dims_employees
    WINDOW w_running AS (ORDER BY department_budget NULLS FIRST)
),
daily AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN department_budget_percent_rank >= 0.65 THEN 'HIGH' ELSE 'LOW' END AS department_budget_percent_rank_tier,
        dept_name,
        COUNT(*) AS event_count,
        ROUND(SUM(department_budget_percent_rank) / NULLIF(COUNT(CASE WHEN dept_name = 'Marketing' THEN 1 END), 0), 2) AS department_budget_percent_rank_per_marketing,
        APPROX_PERCENTILE(department_budget, ARRAY(0.25, 0.5, 0.75)) AS department_budget_quartiles,
        COUNT(CASE WHEN dept_name = 'HR' THEN 1 END) AS hr_count,
        APPROX_PERCENTILE(department_budget, 0.9) AS department_budget_p90,
        ROUND(SUM(row_seq) / NULLIF(COUNT(CASE WHEN location = 'New York' THEN 1 END), 0), 2) AS row_seq_per_new_york
    FROM metrics
    GROUP BY department_budget_percent_rank_tier, dept_name
)
SELECT
    department_budget_percent_rank_tier,
    dept_name,
    event_count,
    department_budget_percent_rank_per_marketing,
    department_budget_quartiles,
    row_seq_per_new_york,
    CASE WHEN dept_name RLIKE '^(HR|Sales)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS dept_name_zone
FROM daily
