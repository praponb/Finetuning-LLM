WITH exploded AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, attr_value
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY host, service ORDER BY status_code, id, k) AS dedup_rank
    FROM exploded
    QUALIFY dedup_rank = 1
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY host, service ORDER BY log_ts, id, k) AS rn
        FROM deduped
    ) x
    WHERE rn = 1
),
metrics AS (
    SELECT
        *,
        LAG(status_code) OVER w_hist AS prev_status_code,
        LEAD(status_code) OVER w_hist AS next_status_code,
        ROW_NUMBER() OVER w_hist AS event_seq,
        COUNT(status_code) OVER (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_code_count_2h
    FROM ranked
    WHERE HOUR(log_ts) BETWEEN 9 AND 13
    WINDOW w_hist AS (PARTITION BY service ORDER BY status_code, id, k)
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY host, service ORDER BY prev_status_code NULLS FIRST, id, k) AS row_num
        FROM metrics
    ) x
    WHERE row_num = 1
),
stats AS (
    SELECT
        host,
        MIN(log_ts) AS started_at,
        SORT_ARRAY(COLLECT_SET(host)) AS host_list
    FROM latest
    GROUP BY 1
)
SELECT
    host,
    started_at,
    host_list
FROM stats
ORDER BY started_at DESC
