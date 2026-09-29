WITH exploded AS (
    SELECT
        id,
        service,
        host,
        latency_ms,
        status_code,
        k,
        v
    FROM app_logs
    LATERAL VIEW EXPLODE(context) t AS k, v
),
flattened_attrs AS (
    SELECT
        id,
        host,
        latency_ms,
        k,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS flag_bit2
    FROM exploded
    WHERE k IN ('region', 'trace')
),
enriched AS (
    SELECT
        id,
        latency_ms,
        k,
        status_code_high,
        LEAD(status_code_high, 1, 0) OVER (ORDER BY flag_bit2, id, k) next_status_code_high,
        LAST_VALUE(latency_ms) IGNORE NULLS OVER (ORDER BY status_code_high, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_latency_ms
    FROM flattened_attrs
    WHERE flag_bit2 = 0
),
latest AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY k ORDER BY status_code_high NULLS LAST) AS dedup_rank
        FROM enriched
    ) ranked
    WHERE dedup_rank = 1
),
scored AS (
    SELECT
        id,
        k,
        status_code_high,
        next_status_code_high,
        last_known_latency_ms,
        dedup_rank,
        AVG(status_code_high) OVER (PARTITION BY k ORDER BY status_code_high, id, k ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) status_code_high_avg_5,
        SUM(latency_ms) OVER (PARTITION BY k) k_sum_latency_ms
    FROM latest
),
base AS (
    SELECT
        id,
        k,
        status_code_high,
        next_status_code_high,
        last_known_latency_ms,
        dedup_rank,
        CAST(status_code_high AS DOUBLE) status_code_high_num,
        COALESCE(k, 'OTHER') resolved_k
    FROM scored
    WHERE k IS NOT NULL
      AND last_known_latency_ms < 1510
),
latest_app_logs AS (
    SELECT *
    FROM base
    QUALIFY RANK() OVER (PARTITION BY k ORDER BY last_known_latency_ms DESC) = 1
)
SELECT
    id,
    k,
    last_known_latency_ms,
    dedup_rank,
    status_code_high_num,
    resolved_k
FROM latest_app_logs;
