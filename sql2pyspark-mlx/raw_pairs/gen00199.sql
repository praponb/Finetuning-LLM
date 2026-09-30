WITH flattened AS (
    SELECT
        id,
        service,
        host,
        log_ts,
        level,
        latency_ms,
        status_code,
        k,
        value
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) x AS k, value
),
flattened_attrs AS (
    SELECT
        service,
        latency_ms,
        status_code,
        value,
        SHIFTRIGHT(status_code, 2) AS status_code_high,
        CAST((status_code & 2) != 0 AS INT) AS flag_bit2
    FROM flattened
    WHERE status_code < 410
),
base AS (
    SELECT
        *,
        ROUND(flag_bit2 * 100, 2) AS flag_bit2_adjusted,
        ROUND(status_code_high / status_code, 2) AS status_code_high_per_status_code
    FROM flattened_attrs
    WHERE status_code_high != 2
),
cleaned AS (
    SELECT
        latency_ms,
        status_code,
        value,
        status_code_high,
        flag_bit2,
        CASE WHEN flag_bit2 >= 1 THEN 'High' WHEN flag_bit2 >= 0 THEN 'Medium' ELSE 'Low' END AS flag_bit2_band,
        LEAST(status_code_high, flag_bit2) AS least_status_code_high_flag_bit2
    FROM base
    WHERE status_code_high_per_status_code > 53
),
grouped AS (
    SELECT
        CASE WHEN latency_ms >= 1977 THEN 'HIGH' ELSE 'LOW' END AS latency_ms_tier,
        CASE WHEN status_code_high >= 2 THEN 'HIGH' ELSE 'LOW' END AS status_code_high_tier,
        PERCENTILE_APPROX(status_code_high, ARRAY(0.25, 0.5, 0.75)) AS status_code_high_quartiles,
        ROUND(AVG(status_code_high), 2) AS avg_status_code_high,
        APPROX_PERCENTILE(status_code_high, 0.5) AS status_code_high_p50,
        AVG(status_code_high) AS avg_status_code_high_2,
        STDDEV_SAMP(status_code) AS status_code_stddev
    FROM cleaned
    GROUP BY CASE WHEN latency_ms >= 1977 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN status_code_high >= 2 THEN 'HIGH' ELSE 'LOW' END
    HAVING STDDEV_SAMP(status_code) > 0
)
SELECT
    latency_ms_tier,
    status_code_high_tier,
    avg_status_code_high_2,
    status_code_stddev
FROM grouped
