WITH unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        log_ts,
        level,
        status_code,
        payload_json,
        context,
        COALESCE(context['version'], 'unknown') AS version_or_default,
        COALESCE(context['trace'], 'direct') AS trace_or_default,
        context['trace'] AS trace,
        context['region'] AS region,
        CAST((status_code & 8) != 0 AS INT) AS is_flagged
    FROM app_logs
    WHERE TO_DATE(log_ts) < '2023-07-01'
)
-- Running and moving aggregates
SELECT
    id,
    log_ts,
    level,
    status_code,
    payload_json,
    context,
    version_or_default,
    trace_or_default,
    trace,
    region,
    is_flagged,
    ROW_NUMBER() OVER (PARTITION BY level ORDER BY status_code DESC, id) AS event_seq,
    PERCENT_RANK() OVER (ORDER BY is_flagged NULLS LAST) AS is_flagged_percent_rank
FROM unpacked
