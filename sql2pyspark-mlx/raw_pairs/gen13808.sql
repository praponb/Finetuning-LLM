WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LOWER(location) AS location_lower,
        COALESCE(dept_name, 'UNKNOWN') AS dept_name_clean
    FROM departments
    WHERE dept_name IS NOT NULL
      AND dept_name != 'Sales'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_name,
        budget,
        location_lower,
        SUM(budget) OVER w_recent AS running_sum_budget,
        MIN(budget) OVER w_recent AS running_min_budget,
        AVG(budget) OVER w_recent AS running_avg_budget,
        MAX(budget) OVER w_recent AS running_max_budget,
        AVG(budget) OVER (ORDER BY budget, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS budget_avg_6
    FROM cleaned
    WINDOW w_recent AS (PARTITION BY dept_name, location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics_departments AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_name,
        budget,
        running_max_budget,
        MAX(budget_avg_6) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_budget_avg_6,
        AVG(budget_avg_6) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_budget_avg_6,
        MIN(budget_avg_6) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_budget_avg_6,
        COUNT(budget_avg_6) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_budget_avg_6,
        SUM(running_sum_budget) OVER (PARTITION BY dept_name ORDER BY running_max_budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_sum_budget
    FROM metrics
),
metrics_departments_2 AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_name,
        running_max_budget,
        running_max_budget_avg_6,
        running_min_budget_avg_6,
        running_count_budget_avg_6,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY running_avg_budget_avg_6 DESC, id) AS event_seq,
        ROUND(running_min_budget_avg_6 / SUM(running_min_budget_avg_6) OVER (PARTITION BY dept_name), 4) AS running_min_budget_avg_6_share
    FROM metrics_departments
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM metrics_departments_2 s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.salary BETWEEN 97000 AND 102000
        )
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.employee_hire_date
    FROM not_flagged s
    LEFT JOIN LATERAL (
            SELECT c.hire_date AS employee_hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
not_flagged_departments AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.hire_date < s.employee_hire_date
        )
),
summary AS (
    -- Grouped metrics
    SELECT
        dept_name,
        AVG(running_max_budget) AS avg_running_max_budget,
        ROUND(SUM(event_seq) / NULLIF(COUNT(CASE WHEN dept_name = 'Engineering' THEN 1 END), 0), 2) AS event_seq_per_engineering,
        COUNT_IF(dept_name = 'Finance') AS finance_events
    FROM not_flagged_departments
    GROUP BY dept_name
)
SELECT
    dept_name,
    avg_running_max_budget,
    event_seq_per_engineering,
    CASE WHEN dept_name RLIKE '^(Engineering|Marketing)$' THEN 'GROUP_A' WHEN dept_name RLIKE '^(Sales|HR)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS dept_name_zone
FROM summary
ORDER BY dept_name_zone DESC, event_seq_per_engineering DESC;
