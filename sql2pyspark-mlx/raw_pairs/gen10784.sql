WITH lines AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, value
),
base AS (
    SELECT
        *,
        INITCAP(TRIM(host)) AS host_title,
        LENGTH(k) AS k_length
    FROM lines
    WHERE log_ts >= DATE '2025-06-01'
      AND k IS NOT NULL
),
enriched AS (
    SELECT
        *,
        MIN(k_length) OVER (PARTITION BY service) AS service_min_k_length,
        MAX(k_length) OVER (PARTITION BY host ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS k_length_max_15m,
        SUM(k_length) OVER (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS k_length_sum_1h
    FROM base
),
metrics AS (
    SELECT
        id,
        host,
        value,
        service_min_k_length,
        PERCENT_RANK() OVER (PARTITION BY YEAR(log_ts), k ORDER BY k_length DESC NULLS FIRST) AS k_length_percent_rank,
        COUNT(service_min_k_length) OVER (PARTITION BY host, k ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS service_min_k_length_count_30m
    FROM enriched
    WHERE log_ts < '2023-05-01'
),
with_windows AS (
    SELECT
        id,
        value,
        service_min_k_length,
        service_min_k_length_count_30m,
        RANK() OVER (PARTITION BY host ORDER BY service_min_k_length DESC NULLS FIRST) AS service_min_k_length_rank,
        PERCENT_RANK() OVER (PARTITION BY host ORDER BY service_min_k_length DESC) AS service_min_k_length_percent_rank,
        CUME_DIST() OVER (ORDER BY k_length_percent_rank) AS k_length_percent_rank_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY host ORDER BY service_min_k_length_count_30m NULLS FIRST) AS service_min_k_length_count_30m_percent_rank,
        COUNT(service_min_k_length) OVER (PARTITION BY host) AS host_count_service_min_k_length
    FROM metrics
)
SELECT
    CASE WHEN service_min_k_length_count_30m >= 16 THEN 'LARGE' ELSE 'SMALL' END AS service_min_k_length_count_30m_tier,
    CASE WHEN service_min_k_length_count_30m_percent_rank >= 0.69 THEN 'HIGH' ELSE 'LOW' END AS service_min_k_length_count_30m_percent_rank_tier,
    ROUND(AVG(k_length_percent_rank_cume_dist), 2) AS avg_k_length_percent_rank_cume_dist,
    COUNT(*) AS num_rows,
    AVG(host_count_service_min_k_length) AS avg_host_count_service_min_k_length,
    AVG(service_min_k_length) AS avg_service_min_k_length,
    PERCENTILE_APPROX(service_min_k_length_count_30m_percent_rank, 0.9) AS service_min_k_length_count_30m_percent_rank_p90,
    ROUND(AVG(service_min_k_length), 2) AS avg_service_min_k_length_2
FROM with_windows
GROUP BY CUBE (CASE WHEN service_min_k_length_count_30m >= 16 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN service_min_k_length_count_30m_percent_rank >= 0.69 THEN 'HIGH' ELSE 'LOW' END)
HAVING avg_service_min_k_length_2 > 0
ORDER BY service_min_k_length_count_30m_percent_rank_p90 DESC
