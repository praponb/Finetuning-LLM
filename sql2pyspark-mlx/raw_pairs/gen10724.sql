WITH kept_keys AS (
    SELECT dept_name, id
    FROM departments
    EXCEPT
    SELECT dept_name, id
    FROM departments
    WHERE dept_name IN ('Finance', 'Engineering')
),
reconciled AS (
    SELECT s.*
    FROM departments s
    INNER JOIN kept_keys k ON s.dept_name = k.dept_name AND s.id = k.id
),
matched_keys AS (
    SELECT dept_name
    FROM reconciled
    EXCEPT
    SELECT dept_name
    FROM departments
    WHERE dept_name = 'Marketing'
),
kept_rows AS (
    SELECT s.*
    FROM reconciled s
    INNER JOIN matched_keys k ON s.dept_name = k.dept_name
),
enriched AS (
    SELECT *, CUME_DIST() OVER (PARTITION BY dept_name ORDER BY budget ASC NULLS FIRST) AS budget_cume_dist
    FROM kept_rows
    WHERE location = 'Singapore'
),
scored AS (
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY dept_name ORDER BY budget_cume_dist DESC) AS budget_cume_dist_cume_dist,
        RANK() OVER (PARTITION BY dept_name ORDER BY budget ASC NULLS FIRST) AS budget_rank
    FROM enriched
),
scored_departments AS (
    SELECT
        id,
        budget,
        ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name, location), 4) AS budget_share
    FROM scored
),
cleaned AS (
    SELECT *, budget / NULLIF(budget_share, 0) AS budget_per_budget_share
    FROM scored_departments
    WHERE budget > 3876000
)
SELECT
    CASE WHEN budget_share >= 0.83 THEN 'HIGH' ELSE 'LOW' END AS budget_share_tier,
    CASE WHEN budget_per_budget_share >= 90 THEN 'HIGH' ELSE 'LOW' END AS budget_per_budget_share_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(budget, 0.9) AS budget_p90,
    COUNT(*) AS num_rows,
    RANK() OVER (PARTITION BY CASE WHEN budget_share >= 0.83 THEN 'HIGH' ELSE 'LOW' END ORDER BY APPROX_PERCENTILE(budget, 0.9) DESC) AS budget_p90_rank
FROM cleaned
GROUP BY CASE WHEN budget_share >= 0.83 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN budget_per_budget_share >= 90 THEN 'HIGH' ELSE 'LOW' END
