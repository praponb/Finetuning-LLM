WITH lines AS (
    SELECT
        id,
        host,
        log_ts,
        level,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) t AS k, value
),
filtered AS (
    SELECT
        id,
        host,
        log_ts,
        level,
        status_code,
        k,
        value,
        CASE WHEN status_code >= 451 THEN 'High' WHEN status_code >= 236 THEN 'Medium' ELSE 'Low' END AS status_code_band
    FROM lines
    WHERE level IN ('ERROR', 'INFO', 'WARN')
),
enriched AS (
    SELECT *, CUME_DIST() OVER (PARTITION BY host ORDER BY status_code) AS status_code_cume_dist
    FROM filtered
),
cleaned AS (
    SELECT
        *,
        ROUND(status_code_cume_dist, 2) AS status_code_cume_dist_rounded,
        status_code + status_code_cume_dist AS status_code_plus_status_code_cume_dist
    FROM enriched
    WHERE log_ts > DATE '2025-06-01'
      AND k IS NOT NULL
)
SELECT
    id,
    k,
    value,
    status_code_cume_dist_rounded,
    status_code_plus_status_code_cume_dist,
    RANK() OVER (PARTITION BY host, level ORDER BY status_code_cume_dist_rounded DESC NULLS FIRST) AS status_code_cume_dist_rounded_rank,
    STDDEV_SAMP(status_code_plus_status_code_cume_dist) OVER (PARTITION BY level) AS level_stddev_samp_status_code_plus_status_code_cume_dist,
    MAX(status_code_cume_dist) OVER (PARTITION BY level) AS level_max_status_code_cume_dist,
    DENSE_RANK() OVER (PARTITION BY TO_DATE(log_ts), host ORDER BY status_code_cume_dist) AS status_code_cume_dist_dense_rank,
    STDDEV_SAMP(status_code_plus_status_code_cume_dist) OVER (PARTITION BY DATE_TRUNC('MONTH', log_ts)) AS log_ts_period_stddev_samp_status_code_plus_status_code_cume_dist
FROM cleaned
