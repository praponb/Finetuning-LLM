WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(location) AS clean_location,
        COALESCE(location, 'N/A') AS location_filled
    FROM departments
    WHERE location IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        location,
        budget,
        clean_location,
        NTILE(4) OVER (ORDER BY budget DESC, id ASC) AS budget_bucket,
        STDDEV_SAMP(budget) OVER (PARTITION BY location) AS location_stddev_samp_budget
    FROM cleaned
),
with_windows_departments AS (
    -- Running and moving aggregates
    SELECT
        id,
        location,
        budget,
        clean_location,
        budget_bucket,
        location_stddev_samp_budget,
        AVG(location_stddev_samp_budget) OVER (ORDER BY budget_bucket ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS location_stddev_samp_budget_avg_3,
        NTILE(10) OVER (PARTITION BY location ORDER BY budget DESC, id ASC) AS budget_bucket_2,
        ROW_NUMBER() OVER (ORDER BY budget_bucket DESC, id ASC) AS event_seq
    FROM with_windows
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(event_seq) OVER (PARTITION BY location ORDER BY event_seq ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS event_seq_avg_5,
        LEAD(location_stddev_samp_budget, 1) OVER (PARTITION BY location ORDER BY location_stddev_samp_budget ASC, id ASC) AS next_location_stddev_samp_budget
    FROM with_windows_departments
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(location_stddev_samp_budget) OVER w_hist AS running_min_location_stddev_samp_budget,
        COUNT(location_stddev_samp_budget) OVER w_hist AS running_count_location_stddev_samp_budget,
        MAX(location_stddev_samp_budget) OVER w_hist AS running_max_location_stddev_samp_budget
    FROM metrics
    WHERE location_stddev_samp_budget BETWEEN 3056048 AND 3507204
    WINDOW w_hist AS (PARTITION BY location ORDER BY event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    budget,
    clean_location,
    budget_bucket,
    location_stddev_samp_budget,
    location_stddev_samp_budget_avg_3,
    event_seq,
    event_seq_avg_5,
    running_count_location_stddev_samp_budget
FROM enriched
ORDER BY running_count_location_stddev_samp_budget ASC NULLS LAST
