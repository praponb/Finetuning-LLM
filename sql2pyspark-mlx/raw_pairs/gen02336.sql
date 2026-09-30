WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        UPPER(location) AS location_upper,
        ROUND(budget * 1.07, 2) AS budget_scaled,
        COALESCE(location, 'NONE') AS location_filled
    FROM departments
    WHERE location IS DISTINCT FROM 'New York'
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.is_remote = TRUE
        )
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.salary
    FROM not_flagged s
    JOIN LATERAL (
            SELECT c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM latest_match
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY budget_scaled DESC, id) = 1
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN salary >= 163000 THEN 'LARGE' ELSE 'SMALL' END AS salary_tier,
        PERCENTILE_APPROX(salary, ARRAY(0.25, 0.5, 0.75)) AS salary_quartiles,
        COUNT(*) AS num_rows,
        AVG(budget_scaled) FILTER (WHERE location = 'Chiang Mai') AS chiang_mai_avg_budget_scaled,
        ROUND(AVG(budget), 2) AS avg_budget,
        SUM(CASE WHEN dept_name = 'Sales' THEN budget_scaled ELSE 0 END) AS sales_budget_scaled
    FROM deduped
    GROUP BY CASE WHEN salary >= 163000 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    salary_tier,
    num_rows,
    chiang_mai_avg_budget_scaled,
    sales_budget_scaled
FROM daily
