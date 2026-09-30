WITH lines AS (
    SELECT
        id,
        log_ts,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, attr_value
),
parsed AS (
    SELECT
        log_ts,
        status_code,
        key,
        attr_value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 1) != 0 AS INT) AS is_flagged
    FROM lines
),
enriched AS (
    SELECT
        *,
        COUNT(status_code_high) OVER (ORDER BY CAST(log_ts AS BIGINT) ASC RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_code_high_count_1h,
        ROUND(status_code / SUM(status_code) OVER (PARTITION BY key), 4) AS status_code_share
    FROM parsed
),
enriched_app_logs AS (
    SELECT
        *,
        SUM(status_code_share) OVER w_running AS key_sum_status_code_share,
        COUNT(is_flagged) OVER w_running AS key_count_is_flagged
    FROM enriched
    WINDOW w_running AS (PARTITION BY key)
),
with_windows AS (
    SELECT
        log_ts,
        status_code,
        key,
        attr_value,
        status_code_high,
        is_flagged,
        status_code_high_count_1h,
        status_code_share,
        key_sum_status_code_share,
        key_count_is_flagged,
        CUME_DIST() OVER (PARTITION BY key ORDER BY is_flagged ASC NULLS LAST) AS is_flagged_cume_dist,
        COUNT(key_count_is_flagged) OVER w_ordered AS key_count_key_count_is_flagged
    FROM enriched_app_logs
    WINDOW w_ordered AS (PARTITION BY key)
)
SELECT
    DATE_TRUNC('DAY', log_ts) AS activity_day,
    CASE WHEN status_code_high >= 2 THEN 'LARGE' ELSE 'SMALL' END AS status_code_high_tier,
    key,
    COUNT(DISTINCT key) AS distinct_keys,
    MAX(is_flagged_cume_dist) AS max_is_flagged_cume_dist,
    SORT_ARRAY(COLLECT_SET(key)) AS key_list,
    (UNIX_TIMESTAMP(MAX(log_ts)) - UNIX_TIMESTAMP(MIN(log_ts))) / 60 AS duration_min,
    COUNT(*) AS cnt,
    COUNT(CASE WHEN key = 'trace' THEN 1 END) AS trace_count
FROM with_windows
GROUP BY activity_day, status_code_high_tier, key
ORDER BY distinct_keys DESC
