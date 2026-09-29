WITH base AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN dept_name = 'Finance' THEN 1 ELSE 0 END AS is_finance
    FROM departments
    WHERE location IS NOT NULL
      AND dept_name IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(budget) OVER (PARTITION BY dept_name, location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_budget,
        MIN(budget) OVER (PARTITION BY dept_name, location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_budget,
        MAX(budget) OVER (PARTITION BY dept_name, location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_budget,
        COUNT(budget) OVER (PARTITION BY dept_name, location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_budget
    FROM base
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(running_max_budget * 1.1, 2) AS running_max_budget_adjusted,
        is_finance + running_max_budget AS is_finance_plus_running_max_budget
    FROM metrics
    WHERE location = 'Chiang Mai'
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.location,
        s.running_count_budget,
        x.hire_date
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.hire_date < s.hire_date
        )
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN running_count_budget >= 17 THEN 'HIGH' ELSE 'LOW' END AS running_count_budget_tier,
    location,
    COUNT(*) FILTER (WHERE location = 'New York') AS new_york_rows,
    ROUND(SUM(running_count_budget) / NULLIF(COUNT(CASE WHEN location = 'Chiang Mai' THEN 1 END), 0), 2) AS running_count_budget_per_chiang_mai,
    SIZE(COLLECT_SET(location)) AS num_locations,
    ROUND(AVG(running_count_budget), 2) AS avg_running_count_budget,
    COUNT(CASE WHEN location = 'Chiang Mai' THEN 1 END) AS chiang_mai_count,
    GROUPING(CASE WHEN running_count_budget >= 17 THEN 'HIGH' ELSE 'LOW' END) AS gid
FROM not_flagged
GROUP BY ROLLUP (CASE WHEN running_count_budget >= 17 THEN 'HIGH' ELSE 'LOW' END, location)
HAVING SIZE(COLLECT_SET(location)) >= 1;
