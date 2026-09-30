WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(budget * 1.07, 2) AS adjusted_budget,
        COALESCE(dept_name, 'N/A') AS resolved_dept_name
    FROM departments
    WHERE dept_name IS DISTINCT FROM 'Marketing'
      AND budget IS NOT NULL
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.hire_date
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.hire_date < s.hire_date
        )
),
kept_departments AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.hire_date >= s.hire_date
        )
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM kept_departments
    EXCEPT
    SELECT id
    FROM departments
    WHERE location IS NOT NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_departments s
    JOIN remaining_keys k ON s.id = k.id
),
kept_departments_2 AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.hire_date <= s.hire_date
        )
),
kept_departments_3 AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept_departments_2 s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.hire_date < '2024-06-22'
        )
),
summary AS (
    -- Grouped metrics
    SELECT
        CASE WHEN adjusted_budget >= 81 THEN 'LARGE' ELSE 'SMALL' END AS adjusted_budget_tier,
        SUM(adjusted_budget) AS total_adjusted_budget,
        PERCENTILE_APPROX(adjusted_budget, 0.9) AS adjusted_budget_p90,
        COUNT(*) AS num_rows,
        ROUND(SUM(budget) / NULLIF(COUNT(CASE WHEN location = 'Bangkok' THEN 1 END), 0), 2) AS budget_per_bangkok,
        COUNT(*) AS num_rows_2,
        ROUND(SUM(adjusted_budget) / NULLIF(COUNT(CASE WHEN location = 'Tokyo' THEN 1 END), 0), 2) AS adjusted_budget_per_tokyo
    FROM kept_departments_3
    GROUP BY 1
)
SELECT
    adjusted_budget_tier,
    total_adjusted_budget,
    num_rows,
    budget_per_bangkok,
    num_rows_2,
    adjusted_budget_per_tokyo
FROM summary
ORDER BY adjusted_budget_per_tokyo NULLS LAST;
