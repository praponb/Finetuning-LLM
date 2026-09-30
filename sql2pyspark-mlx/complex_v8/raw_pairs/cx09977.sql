WITH filtered AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        CASE WHEN quality IN ('BAD', 'EST') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage
    FROM meter_readings
    WHERE (kwh <= 109.0 OR read_at >= TIMESTAMP '2025-02-06 10:00:00')
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count,
        (SELECT MAX(m.read_at) FROM meter_readings m WHERE m.site_id = s.site_id) AS last_reading_read_at
    FROM filtered s
),
with_parent AS (
    -- Attach site attributes
    SELECT s.*, si.site_name
    FROM with_counts s
    LEFT JOIN sites si ON si.id = s.site_id
)
SELECT
    id,
    read_at,
    quality_group,
    kwh_per_voltage,
    reading_count,
    site_name
FROM with_parent
ORDER BY reading_count
