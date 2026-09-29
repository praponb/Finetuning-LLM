WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, v
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(status_code * 1.1, 2) status_code_scaled,
        INITCAP(TRIM(key)) key_title
    FROM exploded
    WHERE host = 'h4'
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, status_code * status_code_scaled status_code_times_status_code_scaled
    FROM valid
    WHERE key IS NOT NULL
      AND key NOT IN ('version', 'trace')
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        key,
        v,
        key_title,
        status_code_times_status_code_scaled,
        RANK() OVER (PARTITION BY key, host ORDER BY status_code ASC) status_code_rank,
        CUME_DIST() OVER (PARTITION BY host ORDER BY status_code ASC) status_code_cume_dist,
        STDDEV_SAMP(status_code_times_status_code_scaled) OVER (PARTITION BY key) key_stddev_samp_status_code_times_status_code_scaled,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY key, host), 4) status_code_share,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY key, host), 4) status_code_share_2
    FROM cleaned
),
cleaned_app_logs AS (
    -- Keep recent, valid rows
    SELECT
        *,
        status_code_cume_dist * key_stddev_samp_status_code_times_status_code_scaled status_code_cume_dist_times_key_stddev_samp_status_code_times_status_code_scaled,
        LEAST(status_code_rank, status_code_times_status_code_scaled) least_status_code_rank_status_code_times_status_code_scaled
    FROM with_windows
    WHERE key IS NOT NULL
)
-- Multi-level aggregation
SELECT
    CASE WHEN status_code_cume_dist >= 0.67 THEN 'HIGH' ELSE 'LOW' END AS status_code_cume_dist_tier,
    CASE WHEN status_code_share_2 >= 0.73 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_share_2_tier,
    COUNT(DISTINCT key) AS distinct_keys,
    SIZE(COLLECT_SET(key)) AS num_keys,
    COUNT(DISTINCT key) AS distinct_keys_2,
    ROUND(AVG(key_stddev_samp_status_code_times_status_code_scaled), 2) AS avg_key_stddev_samp_status_code_times_status_code_scaled,
    COUNT_IF(key = 'region') AS region_events,
    COUNT(CASE WHEN key = 'trace' THEN 1 END) AS trace_count
FROM cleaned_app_logs
GROUP BY GROUPING SETS ((CASE WHEN status_code_cume_dist >= 0.67 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN status_code_share_2 >= 0.73 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN status_code_cume_dist >= 0.67 THEN 'HIGH' ELSE 'LOW' END), ())
HAVING ROUND(AVG(key_stddev_samp_status_code_times_status_code_scaled), 2) > 0
ORDER BY trace_count ASC;
