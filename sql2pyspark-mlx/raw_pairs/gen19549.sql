WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        temp_c,
        humidity,
        pressure,
        status_bits,
        SUBSTRING(firmware, 1, 2) AS firmware_prefix,
        ROUND(temp_c, 2) AS temp_c_rounded
    FROM device_readings
    WHERE humidity >= 62
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(4) OVER (ORDER BY status_bits ASC, id ASC) AS status_bits_bucket,
        ROW_NUMBER() OVER w_ordered AS seq_no,
        NTILE(4) OVER (ORDER BY status_bits DESC, id ASC) AS status_bits_bucket_2,
        RANK() OVER (ORDER BY temp_c_rounded DESC) AS temp_c_rounded_rank,
        LAST_VALUE(temp_c_rounded, TRUE) OVER (ORDER BY status_bits ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_temp_c_rounded
    FROM valid
    WHERE pressure BETWEEN 1015 AND 1021
    WINDOW w_ordered AS (ORDER BY temp_c DESC, id ASC)
),
valid_device_readings AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        humidity,
        pressure,
        status_bits,
        firmware_prefix,
        temp_c_rounded,
        status_bits_bucket,
        seq_no,
        status_bits_bucket_2,
        temp_c_rounded_rank / NULLIF(temp_c_rounded, 0) AS temp_c_rounded_rank_per_temp_c_rounded
    FROM metrics
    WHERE status_bits BETWEEN 9 AND 11
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(status_bits_bucket_2, 0) AS status_bits_bucket_2_rounded,
        CASE WHEN status_bits_bucket_2 >= 7 THEN 'High' WHEN status_bits_bucket_2 >= 4 THEN 'Medium' ELSE 'Low' END AS status_bits_bucket_2_tier
    FROM valid_device_readings
    WHERE status_bits_bucket BETWEEN 7 AND 8
)
SELECT
    id,
    pressure,
    status_bits,
    firmware_prefix,
    temp_c_rounded,
    status_bits_bucket_2,
    status_bits_bucket_2_rounded,
    status_bits_bucket_2_tier
FROM cleaned
