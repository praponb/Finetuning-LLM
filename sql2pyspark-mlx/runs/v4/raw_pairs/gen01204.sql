WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(location, ', ', dept_name) AS location_dept_name,
        COALESCE(location, 'NONE') AS location_filled
    FROM departments
    WHERE location != 'Singapore'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        dept_name,
        location,
        budget,
        location_dept_name,
        location_filled,
        AVG(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS budget_avg_5_prior,
        NTILE(4) OVER (PARTITION BY location ORDER BY budget DESC, id) AS budget_bucket,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS row_seq,
        MAX(budget) OVER (PARTITION BY location, dept_name) AS location_max_budget
    FROM valid
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.job_title = 'Designer'
        )
),
valid_departments AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        dept_name,
        location,
        budget,
        location_dept_name,
        location_filled,
        budget_avg_5_prior,
        budget_bucket,
        row_seq,
        CEIL(budget_avg_5_prior) AS budget_avg_5_prior_whole,
        COALESCE(location, 'NONE') AS location_filled_2
    FROM not_flagged
    WHERE location IS NOT NULL
      AND row_seq > 5
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget_avg_5_prior_whole DESC NULLS LAST, id) AS rn
        FROM valid_departments
    ) ranked
    WHERE rn = 1
),
scored_departments AS (
    -- Running and moving aggregates
    SELECT
        id,
        dept_name,
        location,
        budget,
        location_dept_name,
        location_filled,
        budget_avg_5_prior,
        budget_bucket,
        row_seq,
        budget_avg_5_prior_whole,
        location_filled_2,
        rn,
        ROUND(row_seq / SUM(row_seq) OVER (PARTITION BY location), 4) AS row_seq_share,
        SUM(budget_avg_5_prior) OVER (PARTITION BY dept_name ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_budget_avg_5_prior,
        ROW_NUMBER() OVER (ORDER BY budget_bucket, id) AS row_seq_2,
        NTILE(5) OVER (PARTITION BY location ORDER BY budget_avg_5_prior, id) AS budget_avg_5_prior_bucket,
        CUME_DIST() OVER (ORDER BY budget_bucket DESC NULLS FIRST) AS budget_bucket_cume_dist
    FROM latest
),
matched_keys AS (
    -- Set intersection on keys
    SELECT location, id
    FROM scored_departments
    INTERSECT ALL
    SELECT location, id
    FROM departments
    WHERE budget > 2340000
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored_departments s
    INNER JOIN matched_keys k ON s.location = k.location AND s.id = k.id
)
SELECT
    dept_name,
    location,
    budget,
    location_dept_name,
    location_filled,
    budget_avg_5_prior,
    row_seq,
    budget_avg_5_prior_whole,
    rn,
    CASE WHEN dept_name RLIKE '^(Legal|Marketing)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS dept_name_zone
FROM kept_rows
ORDER BY dept_name_zone DESC NULLS LAST
