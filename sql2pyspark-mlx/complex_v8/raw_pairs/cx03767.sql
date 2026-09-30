WITH base_readings AS (
    -- Scope the meter_readings rows
    SELECT *
    FROM meter_readings
    WHERE read_at < TIMESTAMP '2025-03-08 10:00:00'
      AND voltage < 240.2
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count
    FROM base_readings s
),
line_level AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.quality AS reading_quality
    FROM profiled s
    JOIN meter_readings m ON m.site_id = s.site_id
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM line_level s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality <> 'BAD'
        )
),
stats AS (
    -- Summarise per group
    SELECT
        TO_DATE(read_at) AS read_at_day,
        MAX(read_at) AS last_read_at,
        ARRAY_SORT(COLLECT_SET(reading_quality)) AS reading_quality_list,
        PERCENTILE_APPROX(reading_voltage, 0.5) AS median_reading_voltage
    FROM qualified
    GROUP BY TO_DATE(read_at)
),
annotated AS (
    -- Derive helper columns
    SELECT
        read_at_day,
        last_read_at,
        median_reading_voltage,
        DATE_FORMAT(read_at_day, 'MMM yyyy') AS read_at_day_label
    FROM stats
)
SELECT
    read_at_day,
    last_read_at,
    median_reading_voltage,
    read_at_day_label
FROM annotated
ORDER BY median_reading_voltage DESC
