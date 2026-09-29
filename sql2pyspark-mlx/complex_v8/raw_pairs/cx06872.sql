WITH recent AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        voltage + kwh AS voltage_plus_kwh
    FROM meter_readings
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, MAX(read_at) AS last_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.last_reading_read_at
    FROM recent s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
with_activity AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM profiled s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh <= 96.8
        )
      AND s.quality = 'OK'
),
with_parent AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality,
        s.last_reading_read_at,
        si.tariff AS site_tariff
    FROM with_activity s
    JOIN sites si ON si.id = s.site_id
    WHERE si.tariff = 'EV'
),
combined_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        si.site_name,
        si.zone AS site_zone,
        si.tariff AS site_tariff_2
    FROM with_parent s
    LEFT JOIN sites si ON si.id = s.site_id
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM combined_details_site s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality = 'OK'
              AND m.read_at >= TIMESTAMP '2025-01-19 13:00:00'
        )
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        kwh - LAG(kwh) OVER (PARTITION BY site_tariff ORDER BY last_reading_read_at DESC, id) AS kwh_change,
        ROUND(CUME_DIST() OVER (PARTITION BY site_id ORDER BY last_reading_read_at ASC, id), 3) AS last_reading_read_at_cume_dist
    FROM qualified
)
SELECT
    id,
    site_name,
    site_tariff_2,
    kwh_change,
    SUBSTRING(TRIM(site_tariff_2), 1, 2) AS site_tariff_2_prefix
FROM ranked
ORDER BY kwh_change NULLS LAST, site_name
