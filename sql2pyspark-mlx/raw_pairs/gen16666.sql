WITH flattened AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        status_code,
        key,
        value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS key, value
),
flattened_attrs AS (
    SELECT
        id,
        service,
        log_ts,
        key,
        value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 1) != 0 AS INT) AS flag_bit1
    FROM flattened
),
enriched AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY key ORDER BY log_ts, id, key) AS row_seq,
        ROW_NUMBER() OVER (ORDER BY log_ts, id, key) AS event_seq,
        LEAD(status_code_high, 1) OVER (PARTITION BY TO_DATE(log_ts), key ORDER BY log_ts, id, key) AS next_status_code_high
    FROM flattened_attrs
),
flagged AS (
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY key ORDER BY log_ts, id, key) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY key ORDER BY log_ts, id, key) + INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM enriched
),
sessionized AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY key ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_gaps AS (
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY service ORDER BY log_ts, id, key) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY service ORDER BY log_ts, id, key) + INTERVAL '45' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM sessionized
),
islands AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY service ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
remaining_keys AS (
    SELECT service, id
    FROM islands
    INTERSECT ALL
    SELECT service, id
    FROM app_logs
    WHERE host = 'h1'
),
verified AS (
    SELECT s.*
    FROM islands s
    JOIN remaining_keys k ON s.service = k.service AND s.id = k.id
)
SELECT
    burst_id,
    MIN(log_ts) AS started_at,
    COUNT(DISTINCT key) AS distinct_keys
FROM verified
GROUP BY burst_id
ORDER BY distinct_keys NULLS LAST;
