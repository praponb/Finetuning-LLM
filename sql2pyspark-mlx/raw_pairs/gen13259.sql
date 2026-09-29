WITH flattened AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS key, v
),
cleaned AS (
    SELECT
        id,
        service,
        host,
        level,
        status_code,
        key,
        v,
        DATEDIFF(CURRENT_DATE(), log_ts) AS days_since_log_ts,
        CASE WHEN host IN ('h4', 'h1') THEN 'Tier 1' ELSE 'Other' END AS host_group
    FROM flattened
    WHERE key IS NOT NULL
      AND latency_ms < 679
),
scored AS (
    SELECT
        *,
        RANK() OVER (ORDER BY status_code DESC) AS status_code_rank,
        PERCENT_RANK() OVER (ORDER BY days_since_log_ts NULLS LAST) AS days_since_log_ts_percent_rank
    FROM cleaned
),
filtered AS (
    SELECT
        id,
        service,
        host,
        key,
        v,
        days_since_log_ts,
        host_group,
        FLOOR(status_code) AS status_code_whole,
        status_code_rank + days_since_log_ts_percent_rank AS status_code_rank_plus_days_since_log_ts_percent_rank
    FROM scored
    WHERE key IS NOT DISTINCT FROM 'version'
)
SELECT
    service,
    key,
    v,
    host_group,
    status_code_whole
FROM filtered
ORDER BY service
