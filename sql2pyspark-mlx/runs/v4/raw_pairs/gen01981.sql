WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        key,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS key, value
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, MONTH(log_ts) log_ts_month
    FROM exploded
    WHERE key IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        key,
        value,
        log_ts_month,
        AVG(latency_ms) OVER w_grp latency_ms_avg_30m,
        SUM(latency_ms) OVER w_recent level_sum_latency_ms,
        STDDEV_SAMP(latency_ms) OVER (PARTITION BY service) service_stddev_samp_latency_ms
    FROM filtered
    WINDOW w_grp AS (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY level)
),
by_type AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            key,
            host,
            log_ts_month
        FROM enriched
    ) src
    PIVOT (
        SUM(log_ts_month) AS total,
        COUNT(1) AS n
        FOR host IN ('h2', 'h4')
    )
)
SELECT
    key,
    h2_total,
    h2_n,
    h4_total,
    h4_n,
    CASE WHEN key RLIKE '^(trace|version)$' THEN 'CORE' ELSE 'GROUP_A' END AS key_zone
FROM by_type
ORDER BY key DESC;
