WITH joined_ranges AS (
    -- Price each reading with the tariff window it falls into
    SELECT
        r.id,
        r.site_id,
        r.read_at,
        r.kwh,
        r.quality,
        st.zone,
        tw.rate,
        ROUND(r.kwh * tw.rate, 2) AS energy_cost
    FROM meter_readings r
    JOIN sites st ON st.id = r.site_id
    JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, MIN(read_at) AS first_reading_read_at
    FROM meter_readings
    GROUP BY site_id
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.first_reading_read_at
    FROM joined_ranges s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
with_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality,
        s.zone,
        s.energy_cost,
        s.first_reading_read_at,
        si.tariff AS site_tariff
    FROM profiled s
    JOIN sites si ON si.id = s.site_id AND si.tariff <> 'TOU'
)
-- Subtotals with CUBE
SELECT
    COALESCE(zone, 'ALL') AS zone,
    COALESCE(quality, 'ALL') AS quality,
    COALESCE(site_tariff, 'ALL') AS site_tariff,
    GROUPING(zone) AS is_total_zone,
    MAX(first_reading_read_at) AS last_first_reading_read_at,
    ROUND(AVG(energy_cost), 2) AS avg_energy_cost
FROM with_details_site
GROUP BY CUBE(zone, quality, site_tariff)
