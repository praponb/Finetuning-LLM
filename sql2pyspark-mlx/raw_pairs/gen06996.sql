WITH lines AS (
    SELECT
        id,
        level,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS key, value
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY status_code ASC, id ASC, key ASC) AS dedup_rank
        FROM lines
    ) x
    WHERE dedup_rank = 1
),
with_windows AS (
    SELECT
        *,
        MIN(status_code) OVER w_part last7_min_status_code,
        MAX(status_code) OVER w_part last7_max_status_code,
        COUNT(status_code) OVER w_part last7_count_status_code
    FROM ranked
    WINDOW w_part AS (PARTITION BY level ORDER BY dedup_rank ASC, id ASC, key ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
ranked_app_logs AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level, key ORDER BY last7_max_status_code DESC, id ASC, key ASC) AS latest_rank
    FROM with_windows
    QUALIFY latest_rank = 1
)
SELECT
    id,
    key,
    value,
    dedup_rank,
    last7_count_status_code,
    CASE WHEN key RLIKE '^(version|trace)$' THEN 'EXTENDED' WHEN key RLIKE '^(region)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS key_zone
FROM ranked_app_logs
ORDER BY key DESC NULLS FIRST;
