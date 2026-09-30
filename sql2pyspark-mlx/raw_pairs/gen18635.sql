WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        host,
        log_ts,
        level,
        status_code,
        k,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) li AS k, attr_value
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        host,
        log_ts,
        level,
        status_code,
        k,
        attr_value,
        SUM(status_code) OVER w_hist AS cumulative_status_code,
        SUM(status_code) OVER w_running AS running_status_code
    FROM exploded
    WINDOW w_hist AS (PARTITION BY host ORDER BY status_code, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY host, k ORDER BY status_code, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(running_status_code) OVER (PARTITION BY k) AS k_sum_running_status_code,
        ROUND(cumulative_status_code / SUM(cumulative_status_code) OVER (PARTITION BY k), 4) AS cumulative_status_code_share,
        DENSE_RANK() OVER (ORDER BY cumulative_status_code DESC NULLS LAST) AS cumulative_status_code_dense_rank,
        SUM(running_status_code) OVER (PARTITION BY k ORDER BY status_code, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_status_code
    FROM scored
    WHERE host IN ('h1', 'h2', 'h4')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        log_ts,
        level,
        k,
        cumulative_status_code,
        running_status_code,
        k_sum_running_status_code,
        cumulative_status_code_dense_rank,
        NTILE(4) OVER (PARTITION BY host, k ORDER BY cumulative_status_code_share DESC, id, k) AS cumulative_status_code_share_bucket,
        CUME_DIST() OVER (PARTITION BY k ORDER BY cumulative_status_code DESC NULLS FIRST) AS cumulative_status_code_cume_dist,
        ROUND(cumulative_running_status_code / SUM(cumulative_running_status_code) OVER (PARTITION BY host, k), 4) AS cumulative_running_status_code_share,
        AVG(cumulative_status_code) OVER (PARTITION BY level ORDER BY status_code, id, k ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS cumulative_status_code_avg_6_prior
    FROM with_windows
)
-- Grouped metrics
SELECT
    DATE_TRUNC('HOUR', log_ts) AS hour_window,
    CASE WHEN cumulative_status_code_dense_rank >= 9 THEN 'HIGH' ELSE 'LOW' END AS cumulative_status_code_dense_rank_tier,
    CASE WHEN cumulative_status_code >= 2195 THEN 'PREMIUM' ELSE 'STANDARD' END AS cumulative_status_code_tier,
    COUNT_IF(level = 'INFO') AS info_events,
    COUNT(*) AS event_count,
    ROUND(SUM(running_status_code) / NULLIF(COUNT(CASE WHEN level = 'DEBUG' THEN 1 END), 0), 2) AS running_status_code_per_debug,
    PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('HOUR', log_ts) ORDER BY ROUND(SUM(running_status_code) / NULLIF(COUNT(CASE WHEN level = 'DEBUG' THEN 1 END), 0), 2) DESC) AS running_status_code_per_debug_percent_rank
FROM metrics
GROUP BY hour_window, cumulative_status_code_dense_rank_tier, cumulative_status_code_tier
ORDER BY cumulative_status_code_dense_rank_tier DESC NULLS LAST;
