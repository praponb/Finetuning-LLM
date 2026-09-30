WITH prepared_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        CASE WHEN kwh >= 89.8 THEN 'A' WHEN kwh >= 34.1 THEN 'B' ELSE 'C' END AS kwh_tier
    FROM meter_readings
    WHERE kwh <= 116.8
),
with_children AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.kwh_tier,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.read_at AS reading_read_at
    FROM prepared_readings s
    JOIN meter_readings m ON m.site_id = s.site_id AND MONTH(m.read_at) IN (1, 3)
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    reading_id,
    reading_quality,
    reading_read_at
FROM with_children
ORDER BY id, kwh NULLS LAST, reading_id
LIMIT 20
