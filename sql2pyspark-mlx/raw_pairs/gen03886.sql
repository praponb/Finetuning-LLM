WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN pressure >= 1025 THEN 'High' WHEN pressure >= 997 THEN 'Medium' ELSE 'Low' END AS pressure_tier,
        LENGTH(device_code) AS device_code_length
    FROM device_readings
    WHERE status_bits != 10
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY device_code ORDER BY pressure DESC, id) AS pressure_bucket,
        NTILE(3) OVER w_hist AS device_code_length_bucket,
        CUME_DIST() OVER (ORDER BY humidity DESC NULLS FIRST) AS humidity_cume_dist,
        AVG(humidity) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS humidity_avg_1h,
        ROW_NUMBER() OVER w_hist_2 AS event_seq
    FROM cleaned
    WHERE pressure <= 1026
    WINDOW w_hist AS (PARTITION BY device_code ORDER BY device_code_length, id), w_hist_2 AS (PARTITION BY device_code ORDER BY reading_ts, id)
),
kept_keys AS (
    -- Set intersection on keys
    SELECT firmware, device_code
    FROM metrics
    INTERSECT ALL
    SELECT firmware, device_code
    FROM device_readings
    WHERE firmware IN ('1.1.0', '2.0.1', '1.0.3')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    JOIN kept_keys k ON s.firmware = k.firmware AND s.device_code = k.device_code
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, CEIL(humidity) AS humidity_whole
    FROM verified
    WHERE reading_ts >= TO_TIMESTAMP('2025-09-01 04:00:00')
)
-- Multi-level aggregation
SELECT
    CASE WHEN humidity_whole >= 80 THEN 'HIGH' ELSE 'LOW' END AS humidity_whole_tier,
    device_code,
    COUNT(*) AS event_count,
    PERCENTILE_APPROX(device_code_length_bucket, ARRAY(0.25, 0.5, 0.75)) AS device_code_length_bucket_quartiles,
    SUM(CASE WHEN device_code = 'DV-03' THEN status_bits ELSE 0 END) AS dv_03_status_bits
FROM filtered
GROUP BY humidity_whole_tier, device_code
HAVING event_count >= 1;
