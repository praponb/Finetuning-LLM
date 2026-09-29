WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device_code,
        temp_c,
        pressure,
        status_bits,
        firmware,
        TO_DATE(reading_ts) AS reading_ts_date,
        CAST(reading_ts AS DATE) AS reading_ts_day
    FROM device_readings
    WHERE reading_ts >= DATE '2025-09-01'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_TRUNC('year', reading_ts_day) AS reading_ts_day_period
    FROM filtered
    WHERE temp_c > 37
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts_day_period - LAG(reading_ts_day_period) OVER (PARTITION BY device_code ORDER BY reading_ts_day_period, id) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM valid
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        temp_c,
        pressure,
        reading_ts_date,
        reading_ts_day_period,
        SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts_day_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
),
filtered_device_readings AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_FORMAT(reading_ts_date, 'MMM yyyy') AS reading_ts_date_label,
        DATEDIFF(CURRENT_DATE(), reading_ts_day_period) / 7 AS weeks_since_reading_ts_day_period
    FROM islands
    WHERE TO_DATE(reading_ts_day_period) >= '2025-08-01'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM filtered_device_readings
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY reading_ts_date DESC, id) = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        pressure,
        reading_ts_date,
        reading_ts_day_period,
        island_id,
        reading_ts_date_label,
        SUM(island_id) OVER (PARTITION BY MONTH(reading_ts_day_period), device_code ORDER BY weeks_since_reading_ts_day_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_island_id,
        SUM(weeks_since_reading_ts_day_period) OVER (PARTITION BY device_code ORDER BY reading_ts_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_weeks_since_reading_ts_day_period,
        NTILE(10) OVER (PARTITION BY device_code ORDER BY weeks_since_reading_ts_day_period, id) AS weeks_since_reading_ts_day_period_bucket
    FROM latest
)
SELECT
    id,
    pressure,
    island_id,
    reading_ts_date_label,
    running_island_id,
    running_weeks_since_reading_ts_day_period,
    weeks_since_reading_ts_day_period_bucket
FROM metrics
ORDER BY pressure
