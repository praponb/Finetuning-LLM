WITH exploded AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) e AS key, attr_value
),
with_windows AS (
    SELECT
        *,
        FIRST_VALUE(key) IGNORE NULLS OVER (ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_key,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY log_ts, id, key) AS seq_no
    FROM exploded
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY host ORDER BY log_ts DESC, id, key) AS rn
    FROM with_windows
    QUALIFY rn = 1
),
flagged AS (
    SELECT *, CASE WHEN log_ts - LAG(log_ts) OVER (PARTITION BY key ORDER BY log_ts, id, key) > INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM ranked
),
sessions_app_logs AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY key ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_windows_app_logs AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        key,
        attr_value,
        first_known_key,
        seq_no,
        rn,
        is_new_burst,
        burst_id,
        ROUND(seq_no / SUM(seq_no) OVER (PARTITION BY host, key), 4) AS seq_no_share,
        LAG(seq_no, 2) OVER (PARTITION BY level ORDER BY seq_no, id, key) AS prev_seq_no,
        ROW_NUMBER() OVER (ORDER BY seq_no DESC, id, key) AS event_seq
    FROM sessions_app_logs
),
enriched AS (
    SELECT
        *,
        LAG(event_seq, 1) OVER (PARTITION BY service ORDER BY rn, id, key) AS prev_event_seq,
        SUM(burst_id) OVER (PARTITION BY key ORDER BY event_seq, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_burst_id,
        SUM(event_seq) OVER (PARTITION BY level, host ORDER BY prev_seq_no, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_event_seq,
        LEAD(event_seq, 1, 0) OVER (PARTITION BY service ORDER BY event_seq, id, key) AS next_event_seq,
        LAST_VALUE(is_new_burst, TRUE) OVER (PARTITION BY level ORDER BY seq_no_share, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_is_new_burst
    FROM with_windows_app_logs
)
SELECT
    id,
    service,
    log_ts,
    level,
    key,
    attr_value,
    first_known_key,
    seq_no,
    rn,
    CASE WHEN service RLIKE '^(checkout|auth)$' THEN 'EXTENDED' WHEN service RLIKE '^(billing|search)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS service_zone
FROM enriched
ORDER BY level NULLS LAST;
