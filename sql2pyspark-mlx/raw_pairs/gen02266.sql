WITH cleaned AS (
    SELECT
        *,
        INITCAP(TRIM(location)) location_title,
        COALESCE(location, 'UNKNOWN') location_clean
    FROM departments
    WHERE dept_name IS NOT NULL
      AND dept_name != 'Legal'
),
enriched AS (
    SELECT
        id,
        dept_name,
        location,
        budget,
        location_title,
        location_clean,
        AVG(budget) OVER (PARTITION BY dept_name ORDER BY budget ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) budget_avg_5_prior,
        LAG(budget, 1) OVER (PARTITION BY location ORDER BY budget ASC, id ASC) prev_budget,
        LAST_VALUE(dept_name, TRUE) OVER (PARTITION BY location ORDER BY budget ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_dept_name
    FROM cleaned
),
remaining_keys AS (
    SELECT id, location
    FROM enriched
    INTERSECT ALL
    SELECT id, location
    FROM departments
    WHERE dept_name = 'Marketing'
),
reconciled AS (
    SELECT s.*
    FROM enriched s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.location = k.location
),
with_windows AS (
    SELECT
        *,
        ROUND(budget_avg_5_prior / SUM(budget_avg_5_prior) OVER (PARTITION BY location), 4) budget_avg_5_prior_share,
        ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name, location), 4) budget_share,
        RANK() OVER (ORDER BY budget ASC) budget_rank,
        COUNT(budget_avg_5_prior) OVER (PARTITION BY location) location_count_budget_avg_5_prior,
        AVG(budget) OVER (PARTITION BY location, dept_name) location_avg_budget
    FROM reconciled
),
eligible AS (
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND LOWER(TRIM(x.full_name)) = 'david lee'
        )
)
SELECT *
FROM (
    SELECT
        location,
        budget_avg_5_prior_share,
        prev_budget,
        budget
    FROM eligible
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR metric_name IN (budget_avg_5_prior_share, prev_budget, budget)
) u;
