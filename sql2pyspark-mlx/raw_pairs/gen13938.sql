WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LENGTH(dept_name) dept_name_length,
        FLOOR(budget) budget_whole
    FROM departments
    WHERE budget < 3382000
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.dept_name,
        s.budget,
        s.budget_whole,
        latest.employee_level
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.level AS employee_level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest_match_departments AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.employee_salary
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.salary AS employee_salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        budget,
        budget_whole,
        employee_level,
        employee_salary,
        PERCENT_RANK() OVER (PARTITION BY dept_name ORDER BY employee_salary DESC NULLS FIRST) employee_salary_percent_rank,
        LAG(employee_salary, 1, 0) OVER (PARTITION BY employee_level ORDER BY budget, id) prev_employee_salary,
        SUM(budget_whole) OVER (PARTITION BY employee_level ORDER BY employee_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_budget_whole
    FROM latest_match_departments
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        budget,
        budget_whole,
        employee_level,
        employee_salary_percent_rank,
        cumulative_budget_whole
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_level ORDER BY cumulative_budget_whole NULLS FIRST, id) = 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN cumulative_budget_whole >= 319 THEN 'HIGH' ELSE 'LOW' END AS cumulative_budget_whole_tier,
    SUM(employee_salary_percent_rank) FILTER (WHERE employee_level = 'L1') AS l1_total_employee_salary_percent_rank,
    SIZE(COLLECT_SET(employee_level)) AS num_employee_levels,
    AVG(cumulative_budget_whole) AS avg_cumulative_budget_whole,
    COUNT_IF(employee_level = 'L2') AS l2_events,
    COUNT(*) FILTER (WHERE employee_level = 'L3') AS l3_rows
FROM latest
GROUP BY ROLLUP (CASE WHEN cumulative_budget_whole >= 319 THEN 'HIGH' ELSE 'LOW' END)
