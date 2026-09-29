WITH exploded AS (
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
    LATERAL VIEW EXPLODE(context) t AS key, value
),
ranked AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        key
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level, host ORDER BY latency_ms DESC, id, key) AS latest_rank
        FROM exploded
    ) x
    WHERE latest_rank = 1
),
scored AS (
    SELECT
        id,
        service,
        log_ts,
        level,
        key,
        FIRST_VALUE(key, TRUE) OVER (PARTITION BY service, key ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_key,
        ROW_NUMBER() OVER (PARTITION BY YEAR(log_ts), service ORDER BY log_ts DESC, id, key) event_seq,
        FIRST_VALUE(key) IGNORE NULLS OVER (PARTITION BY level, service ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_key_2,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY log_ts, id, key) seq_no
    FROM ranked
),
flagged AS (
    SELECT *, CASE WHEN LAG(log_ts) OVER (PARTITION BY service ORDER BY log_ts, id, key) IS NULL
                 OR log_ts > LAG(log_ts) OVER (PARTITION BY service ORDER BY log_ts, id, key) + INTERVAL 1 HOUR THEN 1 ELSE 0 END AS is_new_session
    FROM scored
),
sessionized AS (
    SELECT
        id,
        service,
        log_ts,
        key,
        first_known_key,
        first_known_key_2,
        SUM(is_new_session) OVER (PARTITION BY service ORDER BY log_ts, id, key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY key ORDER BY session_no DESC, id, key) AS rn
        FROM sessionized
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    log_ts,
    key,
    first_known_key_2,
    rn
FROM latest
ORDER BY rn NULLS FIRST, id;
