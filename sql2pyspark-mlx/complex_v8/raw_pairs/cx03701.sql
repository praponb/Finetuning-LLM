WITH filtered AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE read_at < TIMESTAMP '2025-03-02 13:00:00'
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM meter_readings m WHERE m.site_id = s.site_id) AS reading_count
    FROM filtered s
),
combined_details_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.reading_count,
        si.zone AS site_zone
    FROM with_stats s
    JOIN sites si ON si.id = s.site_id
    WHERE si.zone = 'Z2'
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT read_at, reading_count, site_zone, kwh
    FROM combined_details_site
) src
PIVOT (
    AVG(kwh) AS avg_v, MAX(kwh) AS max_v
    FOR site_zone IN ('Z4', 'Z3', 'Z2')
)
ORDER BY read_at, reading_count
