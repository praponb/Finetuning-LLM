WITH lines AS (
    SELECT
        id,
        service,
        log_ts,
        latency_ms,
        status_code,
        key,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS key, v
),
flattened_attrs AS (
    SELECT
        service,
        log_ts,
        status_code,
        key,
        CAST((status_code & 8) != 0 AS INT) AS has_fault,
        SHIFTRIGHT(status_code, 2) AS status_code_high
    FROM lines
),
cleaned AS (
    SELECT
        service,
        log_ts,
        status_code,
        key,
        status_code_high,
        status_code / NULLIF(has_fault, 0) AS status_code_per_has_fault,
        COALESCE(key, 'DIRECT') AS resolved_key
    FROM flattened_attrs
    WHERE log_ts > DATE '2025-06-01'
      AND key IS NOT NULL
),
with_windows AS (
    SELECT
        log_ts,
        status_code,
        COUNT(status_code_high) OVER (PARTITION BY service ORDER BY CAST(log_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS status_code_high_count_2h,
        STDDEV_SAMP(status_code) OVER (PARTITION BY service) AS service_stddev_samp_status_code
    FROM cleaned
),
summary AS (
    SELECT
        CASE WHEN service_stddev_samp_status_code >= 414 THEN 'LARGE' ELSE 'SMALL' END AS service_stddev_samp_status_code_tier,
        CASE WHEN status_code_high_count_2h >= 13 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_code_high_count_2h_tier,
        BIT_OR(status_code) AS status_code_bit_or,
        COUNT(*) AS cnt,
        SUM(status_code_high_count_2h) AS total_status_code_high_count_2h,
        APPROX_PERCENTILE(service_stddev_samp_status_code, ARRAY(0.25, 0.5, 0.75)) AS service_stddev_samp_status_code_quartiles,
        PERCENTILE_APPROX(status_code_high_count_2h, ARRAY(0.25, 0.5, 0.75)) AS status_code_high_count_2h_quartiles,
        APPROX_PERCENTILE(service_stddev_samp_status_code, 0.9) AS service_stddev_samp_status_code_p90
    FROM with_windows
    GROUP BY CASE WHEN service_stddev_samp_status_code >= 414 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN status_code_high_count_2h >= 13 THEN 'PREMIUM' ELSE 'STANDARD' END
    HAVING total_status_code_high_count_2h > 0
)
SELECT
    service_stddev_samp_status_code_tier,
    status_code_high_count_2h_tier,
    status_code_bit_or,
    cnt,
    total_status_code_high_count_2h,
    service_stddev_samp_status_code_quartiles,
    status_code_high_count_2h_quartiles,
    service_stddev_samp_status_code_p90
FROM summary
ORDER BY cnt
