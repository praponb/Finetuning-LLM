WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device_code,
        temp_c,
        humidity,
        pressure,
        status_bits,
        SUBSTRING(device_code, 1, 2) AS device_code_prefix
    FROM device_readings
    WHERE TO_DATE(reading_ts) >= '2024-06-01'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure DESC, id) AS dedup_rank
        FROM filtered
    ) x
    WHERE dedup_rank = 1
),
filtered_device_readings AS (
    -- Basic cleansing and defaults
    SELECT *, status_bits * temp_c AS status_bits_times_temp_c
    FROM ranked
    WHERE humidity >= 42
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        status_bits,
        device_code_prefix,
        dedup_rank,
        status_bits_times_temp_c,
        AVG(humidity) OVER w_hist AS running_avg_humidity,
        MIN(humidity) OVER w_hist AS running_min_humidity,
        MAX(humidity) OVER w_hist AS running_max_humidity
    FROM filtered_device_readings
    WINDOW w_hist AS (PARTITION BY device_code ORDER BY status_bits_times_temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered_device_readings_2 AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TRIM(device_code) AS clean_device_code,
        ROUND(running_avg_humidity / dedup_rank, 2) AS running_avg_humidity_per_dedup_rank
    FROM metrics
    WHERE status_bits_times_temp_c BETWEEN 23 AND 79
)
-- Multi-level aggregation
SELECT
    CASE WHEN running_max_humidity >= 66 THEN 'LARGE' ELSE 'SMALL' END AS running_max_humidity_tier,
    AVG(running_min_humidity) FILTER (WHERE device_code = 'DV-02') AS dv_02_avg_running_min_humidity,
    COUNT(*) FILTER (WHERE device_code = 'DV-04') AS dv_04_rows
FROM filtered_device_readings_2
GROUP BY CASE WHEN running_max_humidity >= 66 THEN 'LARGE' ELSE 'SMALL' END
HAVING dv_04_rows > 1;
