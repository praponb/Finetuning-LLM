WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        level,
        latency_ms,
        status_code,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) e AS key, attr_value
),
unpacked AS (
    -- Higher-order array functions
    SELECT
        service,
        latency_ms,
        key,
        attr_value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS is_flagged
    FROM exploded
    WHERE service = 'billing'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (PARTITION BY key ORDER BY status_code_high DESC NULLS LAST) AS status_code_high_rank,
        DENSE_RANK() OVER (PARTITION BY service ORDER BY is_flagged DESC NULLS LAST) AS is_flagged_dense_rank,
        PERCENT_RANK() OVER (ORDER BY is_flagged DESC NULLS LAST) AS is_flagged_percent_rank,
        PERCENT_RANK() OVER (ORDER BY latency_ms DESC) AS latency_ms_percent_rank,
        AVG(status_code_high) OVER (PARTITION BY service) AS service_avg_status_code_high
    FROM unpacked
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY latency_ms DESC) AS latency_ms_cume_dist,
        PERCENT_RANK() OVER (ORDER BY is_flagged_dense_rank DESC NULLS FIRST) AS is_flagged_dense_rank_percent_rank
    FROM with_windows
)
SELECT
    key,
    attr_value,
    status_code_high,
    is_flagged,
    status_code_high_rank,
    is_flagged_dense_rank,
    is_flagged_percent_rank,
    service_avg_status_code_high,
    latency_ms_cume_dist
FROM enriched;
