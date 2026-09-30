WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        latency_ms,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, attr_value
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER w_running AS latency_ms_cume_dist,
        CORR(latency_ms, status_code) OVER (PARTITION BY k) AS corr_latency_ms_status_code,
        PERCENT_RANK() OVER (ORDER BY latency_ms) AS latency_ms_percent_rank
    FROM flattened
    WHERE status_code > 375
    WINDOW w_running AS (PARTITION BY k ORDER BY latency_ms NULLS FIRST)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CEIL(corr_latency_ms_status_code) AS corr_latency_ms_status_code_whole,
        latency_ms_cume_dist - latency_ms_percent_rank AS latency_ms_cume_dist_minus_latency_ms_percent_rank
    FROM enriched
    WHERE latency_ms_percent_rank <= 0.71
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (ORDER BY latency_ms_cume_dist_minus_latency_ms_percent_rank NULLS FIRST) AS latency_ms_cume_dist_minus_latency_ms_percent_rank_rank,
        COUNT(status_code) OVER (PARTITION BY service) AS service_count_status_code
    FROM base
)
-- Window metrics over several different partitions
SELECT
    id,
    host,
    latency_ms,
    k,
    latency_ms_cume_dist,
    corr_latency_ms_status_code,
    corr_latency_ms_status_code_whole,
    service_count_status_code,
    PERCENT_RANK() OVER (PARTITION BY host, service ORDER BY latency_ms_percent_rank DESC NULLS FIRST) AS latency_ms_percent_rank_percent_rank,
    RANK() OVER (ORDER BY status_code DESC) AS status_code_rank,
    CORR(latency_ms, status_code) OVER w_grp AS corr_latency_ms_status_code_2
FROM with_windows
WINDOW w_grp AS (PARTITION BY service, k);
