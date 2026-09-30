WITH scoped_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        CASE WHEN quality IN ('BAD', 'EST') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group,
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_label
    FROM meter_readings
    WHERE kwh < 113.1
),
detail_rows_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality_group,
        s.read_at_label,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.kwh AS reading_kwh,
        m.read_at AS reading_read_at
    FROM scoped_readings s
    JOIN meter_readings m ON m.site_id = s.site_id
)
SELECT
    id,
    site_id,
    read_at,
    quality_group,
    read_at_label,
    reading_quality,
    reading_kwh,
    reading_read_at,
    COALESCE(reading_kwh, 0.0) AS reading_kwh_filled
FROM detail_rows_readings
ORDER BY read_at, reading_kwh_filled
