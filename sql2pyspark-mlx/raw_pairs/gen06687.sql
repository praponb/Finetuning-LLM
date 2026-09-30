WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        status_bits * pressure AS status_bits_times_pressure,
        CASE WHEN firmware = '2.0.1' THEN 1 ELSE 0 END AS is_2_0_1
    FROM device_readings
    WHERE reading_ts < '2024-08-17'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(status_bits_times_pressure) OVER (PARTITION BY device_code ORDER BY status_bits, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_status_bits_times_pressure,
        COUNT(status_bits_times_pressure) OVER (PARTITION BY device_code ORDER BY status_bits, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_status_bits_times_pressure,
        AVG(status_bits_times_pressure) OVER (PARTITION BY device_code ORDER BY status_bits, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_status_bits_times_pressure,
        MIN(status_bits_times_pressure) OVER (PARTITION BY device_code ORDER BY status_bits, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_status_bits_times_pressure,
        STDDEV_SAMP(status_bits_times_pressure) OVER (PARTITION BY device_code) AS device_code_stddev_samp_status_bits_times_pressure
    FROM cleaned
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        STDDEV_SAMP(status_bits) OVER (PARTITION BY device_code) AS device_code_stddev_samp_status_bits,
        MAX(last4_min_status_bits_times_pressure) OVER (PARTITION BY device_code) AS device_code_max_last4_min_status_bits_times_pressure,
        FIRST_VALUE(status_bits_times_pressure, TRUE) OVER (ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status_bits_times_pressure,
        ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity, id) AS event_seq,
        COUNT(pressure) OVER (PARTITION BY device_code) AS device_code_count_pressure
    FROM with_windows
    WHERE device_code_stddev_samp_status_bits_times_pressure < 26
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits DESC, id) = 1
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits_times_pressure DESC NULLS FIRST, id) AS rn
        FROM deduped
    ) x
    WHERE rn = 1
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN pressure >= 1025 THEN 'LARGE' ELSE 'SMALL' END AS pressure_tier,
    device_code,
    SUM(device_code_stddev_samp_status_bits_times_pressure) FILTER (WHERE device_code = 'DV-03') AS dv_03_total_device_code_stddev_samp_status_bits_times_pressure,
    COUNT(DISTINCT firmware) AS distinct_firmwares,
    AVG(status_bits_times_pressure) AS avg_status_bits_times_pressure,
    SUM(CASE WHEN device_code = 'DV-01' THEN last4_max_status_bits_times_pressure ELSE 0 END) AS dv_01_last4_max_status_bits_times_pressure,
    AVG(status_bits_times_pressure) FILTER (WHERE device_code = 'DV-02') AS dv_02_avg_status_bits_times_pressure
FROM latest
GROUP BY pressure_tier, device_code
