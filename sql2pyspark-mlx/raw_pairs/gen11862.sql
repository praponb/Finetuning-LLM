WITH exploded AS (
    SELECT
        id,
        service,
        level,
        latency_ms,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, attr_value
)
SELECT
    id,
    service,
    latency_ms,
    RANK() OVER w_running AS latency_ms_rank,
    RANK() OVER w_running AS latency_ms_rank_2,
    RANK() OVER w_hist AS latency_ms_rank_3,
    CUME_DIST() OVER (PARTITION BY k ORDER BY latency_ms) AS latency_ms_cume_dist
FROM exploded
WINDOW w_running AS (ORDER BY latency_ms), w_hist AS (PARTITION BY service, k ORDER BY latency_ms NULLS LAST);
