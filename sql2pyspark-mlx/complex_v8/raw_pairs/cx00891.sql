WITH scoped_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        DATE_FORMAT(read_at, 'MMM yyyy') AS read_at_label
    FROM meter_readings
    WHERE voltage <= 235.0
      AND kwh <= 81.1
),
with_children AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.read_at_label,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.quality AS reading_quality
    FROM scoped_readings s
    JOIN meter_readings m ON m.site_id = s.site_id
),
with_activity AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM with_children s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
        )
),
enriched_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.read_at_label,
        s.reading_id,
        si.tariff AS site_tariff,
        m.voltage AS reading_voltage,
        m.read_at AS reading_read_at_2
    FROM with_activity s
    JOIN sites si ON si.id = s.site_id AND si.tariff <> 'FLAT'
    LEFT JOIN meter_readings m ON m.id = s.reading_id
)
SELECT
    id,
    site_id,
    read_at,
    reading_id,
    site_tariff,
    reading_voltage,
    reading_read_at_2
FROM enriched_site
ORDER BY site_id DESC
