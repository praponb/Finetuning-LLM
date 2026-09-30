WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(pressure) OVER w_running AS running_count_pressure,
        SUM(pressure) OVER w_running AS running_sum_pressure,
        AVG(pressure) OVER w_running AS running_avg_pressure,
        MIN(pressure) OVER w_running AS running_min_pressure,
        ROUND(humidity / SUM(humidity) OVER (PARTITION BY device_code), 4) AS humidity_share
    FROM device_readings
    WINDOW w_running AS (PARTITION BY device_code ORDER BY status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows_device_readings AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(humidity_share) OVER w_recent AS running_max_humidity_share,
        MIN(humidity_share) OVER w_recent AS running_min_humidity_share,
        SUM(humidity_share) OVER w_recent AS running_sum_humidity_share
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY device_code ORDER BY running_sum_pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        pressure,
        status_bits,
        firmware,
        running_count_pressure,
        running_min_pressure,
        humidity_share,
        running_max_humidity_share,
        SUM(temp_c) OVER (PARTITION BY device_code ORDER BY running_max_humidity_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_temp_c,
        AVG(pressure) OVER (PARTITION BY DATE_TRUNC('week', reading_ts), device_code ORDER BY running_sum_pressure, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS pressure_avg_2_prior,
        LAG(running_sum_pressure, 1) OVER w_grp AS prev_running_sum_pressure,
        ROUND(temp_c / SUM(temp_c) OVER (PARTITION BY device_code), 4) AS temp_c_share,
        SUM(humidity) OVER (PARTITION BY device_code) AS device_code_sum_humidity
    FROM with_windows_device_readings
    WHERE device_code IN ('DV-04', 'DV-03', 'DV-01')
    WINDOW w_grp AS (PARTITION BY device_code ORDER BY pressure, id)
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        STDDEV_SAMP(device_code_sum_humidity) OVER (PARTITION BY device_code) AS device_code_stddev_samp_device_code_sum_humidity,
        AVG(running_max_humidity_share) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS running_max_humidity_share_avg_2h,
        AVG(running_min_pressure) OVER (PARTITION BY device_code ORDER BY running_max_humidity_share, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS running_min_pressure_avg_6_prior,
        LEAD(running_count_pressure, 1) OVER (ORDER BY pressure_avg_2_prior, id) AS next_running_count_pressure,
        AVG(temp_c) OVER (PARTITION BY device_code ORDER BY running_temp_c, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS temp_c_avg_6
    FROM metrics
)
SELECT
    id,
    device_code,
    temp_c,
    firmware,
    running_count_pressure,
    running_min_pressure,
    humidity_share,
    running_max_humidity_share,
    running_temp_c
FROM enriched
