WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(dept_name) AS clean_dept_name,
        SUBSTRING(dept_name, 1, 3) AS dept_name_prefix
    FROM departments
    WHERE location IN ('Singapore', 'New York', 'Tokyo')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER w_grp AS budget_percent_rank,
        ROUND(budget / SUM(budget) OVER (PARTITION BY dept_name, location), 4) AS budget_share
    FROM base
    WINDOW w_grp AS (PARTITION BY location, dept_name ORDER BY budget DESC)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget, id) AS row_seq,
        CUME_DIST() OVER (ORDER BY budget_percent_rank) AS budget_percent_rank_cume_dist,
        NTILE(3) OVER (PARTITION BY location ORDER BY budget DESC, id) AS budget_bucket,
        NTILE(3) OVER (PARTITION BY location, dept_name ORDER BY budget_percent_rank DESC, id) AS budget_percent_rank_bucket,
        CUME_DIST() OVER (ORDER BY budget_percent_rank DESC NULLS FIRST) AS budget_percent_rank_cume_dist_2
    FROM metrics
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        budget_share - budget_percent_rank_bucket AS budget_share_minus_budget_percent_rank_bucket,
        budget_percent_rank_cume_dist_2 - budget_bucket AS budget_percent_rank_cume_dist_2_minus_budget_bucket
    FROM with_windows
    WHERE dept_name IS NOT NULL
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.level = 'L4'
        )
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY dept_name, location ORDER BY budget_percent_rank_bucket DESC) AS latest_rank
        FROM not_flagged
    ) x
    WHERE latest_rank = 1
)
-- Multi-level aggregation
SELECT
    location,
    COUNT(DISTINCT location) AS distinct_locations,
    ROUND(SUM(budget_percent_rank_cume_dist) / NULLIF(COUNT(CASE WHEN location = 'Tokyo' THEN 1 END), 0), 2) AS budget_percent_rank_cume_dist_per_tokyo,
    SORT_ARRAY(COLLECT_SET(location)) AS location_list,
    ROUND(SUM(budget_share) / NULLIF(COUNT(CASE WHEN dept_name = 'Legal' THEN 1 END), 0), 2) AS budget_share_per_legal
FROM deduped
GROUP BY location
HAVING ROUND(SUM(budget_share) / NULLIF(COUNT(CASE WHEN dept_name = 'Legal' THEN 1 END), 0), 2) > 0
ORDER BY distinct_locations DESC NULLS FIRST;
