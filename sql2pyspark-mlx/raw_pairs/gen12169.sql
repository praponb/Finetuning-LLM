WITH filtered AS (
    SELECT
        *,
        LOWER(dept_name) AS dept_name_lower,
        COALESCE(dept_name, 'OTHER') AS dept_name_filled
    FROM departments
    WHERE budget < 3140000
),
with_windows AS (
    SELECT
        id,
        dept_name,
        ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name, location), 4) AS budget_share,
        LEAD(budget, 1) OVER (PARTITION BY location, dept_name ORDER BY budget, id) AS next_budget,
        LAST_VALUE(budget) IGNORE NULLS OVER w_part AS last_known_budget,
        ROUND(budget / SUM(budget) OVER (PARTITION BY location), 4) AS budget_share_2
    FROM filtered
    WHERE dept_name NOT IN ('Marketing', 'Finance')
    WINDOW w_part AS (PARTITION BY location, dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
kept_keys AS (
    SELECT id, dept_name
    FROM with_windows
    INTERSECT ALL
    SELECT id, dept_name
    FROM departments
    WHERE dept_name = 'Marketing'
),
verified AS (
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.id = k.id AND s.dept_name = k.dept_name
),
valid AS (
    SELECT
        id,
        dept_name,
        budget_share,
        next_budget,
        budget_share_2,
        ROUND(last_known_budget * 0.85, 2) AS last_known_budget_scaled,
        COALESCE(dept_name, 'UNKNOWN') AS dept_name_clean
    FROM verified
    WHERE dept_name IS NOT NULL
),
scored AS (
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY dept_name ORDER BY last_known_budget_scaled DESC NULLS FIRST) AS last_known_budget_scaled_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY dept_name ORDER BY last_known_budget_scaled DESC NULLS LAST) AS last_known_budget_scaled_percent_rank_2,
        MIN(budget_share_2) OVER (PARTITION BY dept_name) AS dept_name_min_budget_share_2,
        ROUND(next_budget / SUM(next_budget) OVER (PARTITION BY dept_name), 4) AS next_budget_share
    FROM valid
)
SELECT
    id,
    dept_name,
    budget_share,
    last_known_budget_scaled,
    last_known_budget_scaled_percent_rank,
    dept_name_min_budget_share_2,
    next_budget_share,
    RANK() OVER (PARTITION BY dept_name ORDER BY dept_name_min_budget_share_2 DESC NULLS LAST) AS dept_name_min_budget_share_2_rank,
    SUM(next_budget) OVER (PARTITION BY dept_name) AS dept_name_sum_next_budget,
    STDDEV_SAMP(last_known_budget_scaled) OVER (PARTITION BY dept_name) AS dept_name_stddev_samp_last_known_budget_scaled
FROM scored;
