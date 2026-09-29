WITH exploded AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS attr_key, value
),
base AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        value,
        CONCAT_WS('-', service, attr_key) AS service_attr_key_key,
        COALESCE(attr_key, 'OTHER') AS resolved_attr_key
    FROM exploded
    WHERE log_ts > TO_TIMESTAMP('2025-06-01 07:00:00')
),
metrics AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        latency_ms,
        status_code,
        attr_key,
        value,
        service_attr_key_key,
        SUM(latency_ms) OVER (PARTITION BY MONTH(log_ts), level ORDER BY status_code ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_latency_ms,
        LAST_VALUE(attr_key) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('week', log_ts), service ORDER BY latency_ms ASC, id ASC, attr_key ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_attr_key,
        ROW_NUMBER() OVER (PARTITION BY attr_key, level ORDER BY latency_ms DESC, id ASC, attr_key ASC) AS event_seq,
        CORR(latency_ms, status_code) OVER (PARTITION BY level) AS corr_latency_ms_status_code,
        PERCENT_RANK() OVER (PARTITION BY attr_key ORDER BY latency_ms DESC NULLS LAST) AS latency_ms_percent_rank
    FROM base
    WHERE TO_DATE(log_ts) < '2024-06-22'
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key, service ORDER BY latency_ms_percent_rank DESC NULLS LAST, id ASC, attr_key ASC) AS row_num
        FROM metrics
    ) x
    WHERE row_num = 1
),
filtered AS (
    SELECT
        id,
        service,
        level,
        latency_ms,
        attr_key,
        value,
        service_attr_key_key,
        running_latency_ms,
        last_known_attr_key,
        corr_latency_ms_status_code,
        latency_ms_percent_rank,
        row_num,
        YEAR(log_ts) AS log_ts_year
    FROM deduped
    WHERE attr_key IS NOT NULL
      AND latency_ms <= 2032
)
SELECT *
FROM (
    SELECT
        service,
        level,
        attr_key,
        latency_ms_percent_rank
    FROM filtered
) src
PIVOT (
    MAX(latency_ms_percent_rank)
    FOR attr_key IN ('region' AS region, 'trace' AS trace, 'version' AS version)
);
