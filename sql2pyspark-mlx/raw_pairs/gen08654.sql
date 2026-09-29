WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        FLOOR(budget) AS budget_whole,
        COALESCE(dept_name, 'UNKNOWN') AS resolved_dept_name
    FROM departments
    WHERE dept_name IS NOT NULL
      AND location IN ('Berlin', 'Bangkok')
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        location,
        SUM(budget_whole) OVER (PARTITION BY dept_name ORDER BY budget_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_budget_whole,
        AVG(budget_whole) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS budget_whole_avg_2_prior,
        AVG(budget_whole) OVER (PARTITION BY dept_name ORDER BY budget_whole, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS budget_whole_avg_3_prior
    FROM cleaned
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        location,
        budget_whole_avg_3_prior
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY location ORDER BY cumulative_budget_whole, id) = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        location,
        AVG(budget_whole_avg_3_prior) OVER w_grp AS budget_whole_avg_3_prior_avg_3_prior,
        RANK() OVER (PARTITION BY location ORDER BY budget_whole_avg_3_prior DESC NULLS FIRST) AS budget_whole_avg_3_prior_rank,
        ROW_NUMBER() OVER (ORDER BY budget_whole_avg_3_prior DESC, id) AS seq_no,
        NTILE(10) OVER (PARTITION BY location ORDER BY budget_whole_avg_3_prior, id) AS budget_whole_avg_3_prior_bucket
    FROM deduped
    WINDOW w_grp AS (PARTITION BY location ORDER BY budget_whole_avg_3_prior, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget_whole_avg_3_prior_avg_3_prior DESC NULLS LAST, id) AS row_num
        FROM enriched
    ) t
    WHERE row_num = 1
)
SELECT
    id,
    location,
    budget_whole_avg_3_prior_avg_3_prior,
    seq_no,
    row_num,
    CASE WHEN location RLIKE '^(New York|Berlin)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS location_zone
FROM ranked
ORDER BY location, location_zone;
