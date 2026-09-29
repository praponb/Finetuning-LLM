WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        latency_ms,
        attr_key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS attr_key, value
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY latency_ms DESC, id, attr_key) event_seq,
        SUM(latency_ms) OVER (PARTITION BY service, attr_key ORDER BY latency_ms, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_latency_ms,
        LEAD(latency_ms, 1) OVER (ORDER BY latency_ms, id, attr_key) next_latency_ms
    FROM exploded
),
with_windows_app_logs AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(event_seq, TRUE) OVER (PARTITION BY attr_key ORDER BY next_latency_ms, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_event_seq,
        NTILE(3) OVER (PARTITION BY attr_key ORDER BY running_latency_ms, id, attr_key) running_latency_ms_bucket
    FROM with_windows
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, SUBSTRING(attr_key, 1, 2) attr_key_prefix
    FROM with_windows_app_logs
    WHERE running_latency_ms < 8453
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN event_seq >= 8 THEN 'LARGE' ELSE 'SMALL' END AS event_seq_tier,
    CASE WHEN last_known_event_seq >= 7 THEN 'HIGH' ELSE 'LOW' END AS last_known_event_seq_tier,
    COUNT(*) FILTER (WHERE service = 'auth') AS auth_rows,
    ROUND(AVG(next_latency_ms), 2) AS avg_next_latency_ms,
    APPROX_PERCENTILE(next_latency_ms, 0.5) AS next_latency_ms_p50,
    GROUPING_ID() AS gid
FROM cleaned
GROUP BY GROUPING SETS ((CASE WHEN event_seq >= 8 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN last_known_event_seq >= 7 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN event_seq >= 8 THEN 'LARGE' ELSE 'SMALL' END))
ORDER BY avg_next_latency_ms NULLS FIRST
