WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS k, attr_value
),
scored AS (
    -- Ranking and gap features
    SELECT
        latency_ms,
        k,
        attr_value,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY k), 4) AS latency_ms_share,
        AVG(status_code) OVER (PARTITION BY k) AS k_avg_status_code,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY k), 4) AS latency_ms_share_2,
        SUM(status_code) OVER (ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS status_code_sum_1h,
        ROUND(latency_ms / SUM(latency_ms) OVER (PARTITION BY MONTH(log_ts), k), 4) AS latency_ms_share_3
    FROM exploded
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        k,
        latency_ms_share,
        k_avg_status_code,
        latency_ms_share_3,
        CAST(latency_ms AS STRING) AS latency_ms_str,
        latency_ms_share_3 + k_avg_status_code AS latency_ms_share_3_plus_k_avg_status_code,
        COALESCE(k, 'N/A') AS k_clean
    FROM scored
    WHERE latency_ms >= 2007
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY latency_ms_share NULLS LAST) AS latency_ms_share_dense_rank,
        DENSE_RANK() OVER (PARTITION BY k ORDER BY latency_ms_share DESC) AS latency_ms_share_dense_rank_2
    FROM filtered
    WHERE latency_ms_share < 0.24
)
SELECT
    latency_ms_share,
    k_avg_status_code,
    latency_ms_share_3,
    k_clean,
    latency_ms_share_dense_rank_2
FROM enriched
ORDER BY k_avg_status_code DESC
