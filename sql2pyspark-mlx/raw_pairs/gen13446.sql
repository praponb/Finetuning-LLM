WITH flattened AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, v
),
cleaned AS (
    SELECT
        service,
        log_ts,
        level,
        key,
        v,
        SUBSTRING(service, 1, 4) AS service_prefix
    FROM flattened
    WHERE log_ts > TO_TIMESTAMP('2025-06-01 14:00:00')
      AND log_ts < '2024-06-01'
),
cleaned_app_logs AS (
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), log_ts) AS days_since_log_ts,
        CASE WHEN level IN ('ERROR', 'DEBUG') THEN 'Priority' ELSE 'Other' END AS level_group
    FROM cleaned
    WHERE log_ts >= DATE '2025-06-01'
),
scored AS (
    SELECT
        log_ts,
        v,
        service_prefix,
        MAX(days_since_log_ts) OVER (PARTITION BY service) AS service_max_days_since_log_ts,
        PERCENT_RANK() OVER (PARTITION BY service ORDER BY days_since_log_ts DESC NULLS LAST) AS days_since_log_ts_percent_rank,
        CUME_DIST() OVER (ORDER BY days_since_log_ts DESC NULLS FIRST) AS days_since_log_ts_cume_dist,
        CUME_DIST() OVER (PARTITION BY MONTH(log_ts), level ORDER BY days_since_log_ts NULLS LAST) AS days_since_log_ts_cume_dist_2
    FROM cleaned_app_logs
),
base AS (
    SELECT *, DATEDIFF(CURRENT_DATE(), log_ts) AS days_since_log_ts
    FROM scored
    WHERE log_ts > TIMESTAMP '2025-06-01 10:00:00'
      AND TO_DATE(log_ts) >= '2023-04-01'
),
filtered AS (
    SELECT *, days_since_log_ts_cume_dist_2 / NULLIF(days_since_log_ts_percent_rank, 0) AS days_since_log_ts_cume_dist_2_per_days_since_log_ts_percent_rank
    FROM base
    WHERE log_ts >= TIMESTAMP '2025-06-01 11:00:00'
      AND days_since_log_ts_percent_rank >= 0.32
)
SELECT
    log_ts,
    v,
    days_since_log_ts_cume_dist,
    days_since_log_ts,
    AVG(days_since_log_ts_cume_dist_2) OVER w_hist AS days_since_log_ts_cume_dist_2_avg_1h
FROM filtered
WHERE days_since_log_ts < 48
WINDOW w_hist AS (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW);
