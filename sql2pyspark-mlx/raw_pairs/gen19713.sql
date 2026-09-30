WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN firmware IN ('1.0.3', '2.0.1') THEN 'Core' ELSE 'Other' END AS firmware_group
    FROM device_readings
    WHERE status_bits != 6
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        reading_ts,
        temp_c,
        pressure,
        status_bits,
        firmware,
        firmware_group,
        ROUND(temp_c / SUM(temp_c) OVER (PARTITION BY device_code), 4) AS temp_c_share,
        NTILE(5) OVER (PARTITION BY device_code ORDER BY humidity, id) AS humidity_bucket,
        ROW_NUMBER() OVER w_recent AS row_seq
    FROM filtered
    WINDOW w_recent AS (PARTITION BY device_code ORDER BY status_bits DESC, id)
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        temp_c,
        status_bits,
        firmware,
        humidity_bucket,
        row_seq,
        TRIM(firmware) AS clean_firmware
    FROM scored
    WHERE reading_ts > DATE '2025-09-01'
),
base_device_readings AS (
    -- Keep recent, valid rows
    SELECT *, SUBSTRING(firmware, 1, 3) AS firmware_prefix
    FROM base
    WHERE row_seq IS NULL
),
base_device_readings_2 AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        status_bits + row_seq AS status_bits_plus_row_seq,
        UPPER(firmware) AS firmware_upper
    FROM base_device_readings
    WHERE temp_c >= 24
),
filtered_device_readings AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        temp_c,
        status_bits,
        firmware,
        humidity_bucket,
        row_seq,
        firmware_prefix,
        status_bits_plus_row_seq,
        firmware_upper,
        status_bits * row_seq AS status_bits_times_row_seq
    FROM base_device_readings_2
    WHERE status_bits_plus_row_seq IS NOT NULL
)
-- Multi-level aggregation
SELECT
    CASE WHEN row_seq >= 7 THEN 'HIGH' ELSE 'LOW' END AS row_seq_tier,
    CASE WHEN status_bits >= 12 THEN 'LARGE' ELSE 'SMALL' END AS status_bits_tier,
    CORR(status_bits, temp_c) AS corr_status_bits_temp_c,
    APPROX_PERCENTILE(humidity_bucket, ARRAY(0.25, 0.5, 0.75)) AS humidity_bucket_quartiles,
    APPROX_PERCENTILE(status_bits_plus_row_seq, 0.9) AS status_bits_plus_row_seq_p90,
    RANK() OVER (PARTITION BY CASE WHEN row_seq >= 7 THEN 'HIGH' ELSE 'LOW' END ORDER BY APPROX_PERCENTILE(status_bits_plus_row_seq, 0.9) DESC) AS status_bits_plus_row_seq_p90_rank
FROM filtered_device_readings
GROUP BY CASE WHEN row_seq >= 7 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN status_bits >= 12 THEN 'LARGE' ELSE 'SMALL' END
HAVING CORR(status_bits, temp_c) < 1000000
ORDER BY corr_status_bits_temp_c DESC;
