WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage <= 240.9
),
latest_per_key AS (
    -- Keep the latest row per quality, site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality, site_id ORDER BY read_at DESC, id DESC) AS recency_rank
        FROM valid
    ) ranked
    WHERE recency_rank = 1
),
exploded_detail AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        m.id AS reading_id,
        m.quality AS reading_quality,
        m.kwh AS reading_kwh
    FROM latest_per_key s
    JOIN meter_readings m ON m.site_id = s.site_id
),
with_parent AS (
    -- Attach reading attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.reading_id,
        s.reading_kwh,
        m.quality AS reading_quality_2,
        m.voltage AS reading_voltage,
        m.read_at AS reading_read_at
    FROM exploded_detail s
    JOIN meter_readings m ON m.id = s.reading_id
    WHERE m.quality NOT IN ('EST', 'OK')
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT reading_id, reading_quality_2, reading_kwh
    FROM with_parent
) src
PIVOT (
    MAX(reading_kwh)
    FOR reading_quality_2 IN ('OK', 'EST')
)
ORDER BY reading_id
