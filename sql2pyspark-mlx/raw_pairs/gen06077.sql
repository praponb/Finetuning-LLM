WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        firmware,
        LENGTH(device_code) AS device_code_length
    FROM device_readings
    WHERE status_bits < 4
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        reading_ts,
        humidity,
        status_bits,
        firmware,
        SUM(pressure) OVER w_grp AS pressure_sum_1h,
        AVG(status_bits) OVER (PARTITION BY DATE_TRUNC('WEEK', reading_ts) ORDER BY pressure, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS status_bits_avg_3,
        SUM(humidity) OVER (ORDER BY status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_humidity
    FROM cleaned
    WHERE humidity > 37
    WINDOW w_grp AS (ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        humidity,
        status_bits,
        firmware,
        pressure_sum_1h,
        running_humidity,
        MAX(running_humidity) OVER w_ordered AS running_max_running_humidity,
        COUNT(running_humidity) OVER w_ordered AS running_count_running_humidity,
        MIN(running_humidity) OVER w_ordered AS running_min_running_humidity
    FROM scored
    WINDOW w_ordered AS (ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, firmware
    FROM metrics
    EXCEPT
    SELECT id, firmware
    FROM device_readings
    WHERE firmware IN ('2.0.1', '1.0.3')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    JOIN remaining_keys k ON s.id = k.id AND s.firmware = k.firmware
),
cleaned_device_readings AS (
    -- Keep recent, valid rows
    SELECT
        id,
        humidity,
        status_bits,
        firmware,
        pressure_sum_1h,
        running_humidity,
        running_max_running_humidity,
        running_count_running_humidity,
        running_min_running_humidity,
        CASE WHEN running_max_running_humidity >= 380 THEN 'High' WHEN running_max_running_humidity >= 140 THEN 'Medium' ELSE 'Low' END AS running_max_running_humidity_tier,
        GREATEST(pressure_sum_1h, running_max_running_humidity) AS greatest_pressure_sum_1h_running_max_running_humidity
    FROM kept_rows
    WHERE running_humidity BETWEEN 225 AND 258
),
metrics_device_readings AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY pressure_sum_1h NULLS FIRST) AS pressure_sum_1h_cume_dist,
        CUME_DIST() OVER (ORDER BY running_humidity NULLS FIRST) AS running_humidity_cume_dist,
        PERCENT_RANK() OVER (ORDER BY status_bits DESC) AS status_bits_percent_rank,
        CUME_DIST() OVER (ORDER BY running_max_running_humidity) AS running_max_running_humidity_cume_dist,
        RANK() OVER (ORDER BY pressure_sum_1h NULLS FIRST) AS pressure_sum_1h_rank
    FROM cleaned_device_readings
),
metrics_device_readings_2 AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY running_humidity DESC NULLS FIRST) AS running_humidity_cume_dist_2,
        CUME_DIST() OVER w_grp AS status_bits_percent_rank_cume_dist
    FROM metrics_device_readings
    WINDOW w_grp AS (ORDER BY status_bits_percent_rank DESC)
)
-- Window metrics over several different partitions
SELECT
    id,
    humidity,
    status_bits,
    firmware,
    running_humidity,
    running_max_running_humidity,
    running_max_running_humidity_tier,
    running_humidity_cume_dist,
    status_bits_percent_rank,
    running_max_running_humidity_cume_dist,
    pressure_sum_1h_rank,
    running_humidity_cume_dist_2,
    RANK() OVER (ORDER BY status_bits) AS status_bits_rank,
    CUME_DIST() OVER (ORDER BY pressure_sum_1h_rank DESC) AS pressure_sum_1h_rank_cume_dist,
    RANK() OVER (ORDER BY status_bits_percent_rank NULLS FIRST) AS status_bits_percent_rank_rank
FROM metrics_device_readings_2
