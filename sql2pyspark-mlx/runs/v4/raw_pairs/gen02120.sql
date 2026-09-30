WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        status_bits * temp_c status_bits_times_temp_c
    FROM device_readings
    WHERE reading_ts > DATE '2025-09-01'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(pressure, 1) OVER (ORDER BY temp_c, id) next_pressure,
        SUM(status_bits_times_temp_c) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_status_bits_times_temp_c,
        ROUND(humidity / SUM(humidity) OVER (PARTITION BY device_code), 4) humidity_share
    FROM filtered
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY running_status_bits_times_temp_c DESC, id) AS row_num
        FROM enriched
    ) ranked
    WHERE row_num = 1
),
stats AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('HOUR', reading_ts) AS hour_window,
        MAX(running_status_bits_times_temp_c) AS max_running_status_bits_times_temp_c,
        APPROX_PERCENTILE(temp_c, ARRAY(0.25, 0.5, 0.75)) AS temp_c_quartiles
    FROM deduped
    GROUP BY 1
    HAVING max_running_status_bits_times_temp_c < 0
)
SELECT
    hour_window,
    max_running_status_bits_times_temp_c,
    temp_c_quartiles
FROM stats
ORDER BY hour_window DESC;
