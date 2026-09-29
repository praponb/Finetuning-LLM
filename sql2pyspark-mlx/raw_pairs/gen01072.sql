WITH lines AS (
    SELECT
        id,
        host,
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS attr_key, attr_value
),
parsed AS (
    SELECT
        log_ts,
        latency_ms,
        status_code,
        attr_key,
        attr_value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 4) != 0 AS INT) AS is_flagged
    FROM lines
),
scored AS (
    SELECT
        latency_ms,
        attr_value,
        status_code_high,
        DENSE_RANK() OVER (PARTITION BY YEAR(log_ts), attr_key ORDER BY status_code_high DESC NULLS FIRST) AS status_code_high_dense_rank
    FROM parsed
    WHERE latency_ms BETWEEN 1068 AND 1920
),
scored_app_logs AS (
    SELECT
        latency_ms,
        attr_value,
        status_code_high,
        RANK() OVER (ORDER BY latency_ms DESC NULLS FIRST) AS latency_ms_rank,
        RANK() OVER (ORDER BY status_code_high_dense_rank DESC) AS status_code_high_dense_rank_rank
    FROM scored
),
scored_app_logs_2 AS (
    SELECT *, DENSE_RANK() OVER (ORDER BY status_code_high_dense_rank_rank DESC NULLS LAST) AS status_code_high_dense_rank_rank_dense_rank
    FROM scored_app_logs
)
SELECT
    latency_ms,
    attr_value,
    status_code_high_dense_rank_rank,
    status_code_high_dense_rank_rank_dense_rank
FROM scored_app_logs_2
