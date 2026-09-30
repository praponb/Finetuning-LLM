WITH filtered_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality
    FROM meter_readings
    WHERE voltage >= 224.8
),
detail_rows_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.quality,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at
    FROM filtered_readings s
    LEFT JOIN meter_readings m ON m.site_id = s.site_id
)
SELECT id, reading_id, field, metric_value
FROM (
    SELECT id, reading_id, reading_kwh, kwh
    FROM detail_rows_readings
) src
UNPIVOT (
    metric_value FOR field IN (reading_kwh, kwh)
) unp
ORDER BY id, reading_id, field
