WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        kwh + voltage AS kwh_plus_voltage
    FROM meter_readings
    WHERE site_id >= 6
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality <> 'BAD'
        )
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.quality,
        s.kwh_plus_voltage,
        si.zone AS site_zone,
        si.site_name
    FROM qualified s
    JOIN sites si ON si.id = s.site_id
),
line_level AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.quality,
        s.kwh_plus_voltage,
        s.site_zone,
        s.site_name,
        m.id AS reading_id,
        m.read_at AS reading_read_at,
        m.voltage AS reading_voltage,
        m.quality AS reading_quality,
        m.kwh AS reading_kwh
    FROM with_details s
    JOIN meter_readings m ON m.site_id = s.site_id AND MONTH(m.read_at) IN (2, 3)
),
enriched_reading AS (
    -- Join meter_readings
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.quality,
        s.site_zone,
        s.site_name,
        s.reading_id,
        s.reading_read_at,
        s.reading_voltage,
        s.reading_quality,
        s.reading_kwh,
        m.site_id AS reading_site_id
    FROM line_level s
    LEFT JOIN meter_readings m ON m.id = s.reading_id
),
qualified_2 AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM enriched_reading s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.reading_site_id
        )
      AND s.reading_id IS NOT NULL
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(reading_read_at) ORDER BY voltage DESC), 3) AS voltage_cume_dist,
        DENSE_RANK() OVER (PARTITION BY YEAR(reading_read_at) ORDER BY voltage DESC, id, reading_id) AS voltage_dense_rank,
        LAST_VALUE(reading_read_at) OVER (PARTITION BY reading_id, reading_site_id ORDER BY reading_kwh ASC, id, reading_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_reading_read_at,
        ROUND(reading_voltage / NULLIF(SUM(reading_voltage) OVER (PARTITION BY quality, reading_id), 0), 4) AS reading_voltage_share_of_quality_reading_id
    FROM qualified_2
)
-- Final projection
SELECT
    id,
    site_id,
    voltage,
    quality,
    site_zone,
    site_name,
    reading_id,
    reading_voltage,
    reading_quality,
    reading_kwh,
    reading_site_id,
    voltage_dense_rank,
    last_reading_read_at,
    reading_voltage_share_of_quality_reading_id
FROM windowed
ORDER BY reading_site_id DESC
