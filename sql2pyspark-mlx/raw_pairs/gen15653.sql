WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC) > INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM device_readings
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT firmware, id
    FROM sessions_device_readings
    INTERSECT ALL
    SELECT firmware, id
    FROM device_readings
    WHERE reading_ts >= '2023-08-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessions_device_readings s
    INNER JOIN remaining_keys k ON s.firmware = k.firmware AND s.id = k.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        reading_ts,
        temp_c,
        pressure,
        firmware,
        is_new_session,
        burst_id,
        ROUND(humidity / SUM(humidity) OVER (PARTITION BY device_code), 4) AS humidity_share,
        CORR(status_bits, pressure) OVER w_grp AS corr_status_bits_pressure
    FROM reconciled
    WHERE humidity <= 34
    WINDOW w_grp AS (PARTITION BY device_code)
),
remaining_keys_2 AS (
    -- Set difference on keys
    SELECT firmware
    FROM with_windows
    EXCEPT ALL
    SELECT firmware
    FROM device_readings
    WHERE HOUR(reading_ts) BETWEEN 6 AND 13
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN remaining_keys_2 k ON s.firmware = k.firmware
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        pressure,
        firmware,
        is_new_session,
        burst_id,
        humidity_share,
        corr_status_bits_pressure,
        CAST(corr_status_bits_pressure AS INT) AS corr_status_bits_pressure_int
    FROM verified
    WHERE reading_ts > DATE '2025-09-01'
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, corr_status_bits_pressure * is_new_session AS corr_status_bits_pressure_times_is_new_session
    FROM base
    WHERE corr_status_bits_pressure >= 0.1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        pressure,
        firmware,
        is_new_session,
        humidity_share,
        corr_status_bits_pressure_int,
        corr_status_bits_pressure_times_is_new_session,
        DENSE_RANK() OVER w_part AS humidity_share_dense_rank,
        PERCENT_RANK() OVER (ORDER BY corr_status_bits_pressure_times_is_new_session DESC NULLS LAST) AS corr_status_bits_pressure_times_is_new_session_percent_rank,
        PERCENT_RANK() OVER w_part_2 AS humidity_share_percent_rank,
        CUME_DIST() OVER w_hist AS corr_status_bits_pressure_int_cume_dist
    FROM filtered
    WINDOW w_part AS (ORDER BY humidity_share DESC), w_part_2 AS (ORDER BY humidity_share ASC NULLS FIRST), w_hist AS (ORDER BY corr_status_bits_pressure_int DESC NULLS LAST)
)
-- Multi-level aggregation
SELECT
    CASE WHEN corr_status_bits_pressure_int_cume_dist >= 0.69 THEN 'HIGH' ELSE 'LOW' END AS corr_status_bits_pressure_int_cume_dist_tier,
    CASE WHEN humidity_share >= 0.61 THEN 'LARGE' ELSE 'SMALL' END AS humidity_share_tier,
    APPROX_PERCENTILE(humidity_share, 0.95) AS humidity_share_p95,
    COUNT(*) AS cnt,
    PERCENTILE_APPROX(pressure, 0.5) AS pressure_p50,
    COUNT(DISTINCT firmware) AS distinct_firmwares,
    COUNT(*) AS cnt_2,
    ROUND(AVG(humidity_share_dense_rank), 2) AS avg_humidity_share_dense_rank
FROM enriched
GROUP BY CASE WHEN corr_status_bits_pressure_int_cume_dist >= 0.69 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN humidity_share >= 0.61 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY avg_humidity_share_dense_rank DESC NULLS LAST;
