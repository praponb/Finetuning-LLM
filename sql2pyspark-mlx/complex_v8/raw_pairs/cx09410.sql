WITH covered AS (
    -- Price each reading with the tariff window it falls into
    SELECT
        r.id,
        r.site_id,
        r.read_at,
        r.kwh,
        r.voltage,
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
    SELECT site_id, ROUND(AVG(kwh), 2) AS avg_reading_kwh
    FROM meter_readings
    GROUP BY site_id
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        m.avg_reading_kwh
    FROM covered s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
),
joined_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.voltage,
        s.zone,
        s.rate,
        s.energy_cost,
        s.avg_reading_kwh,
        si.commissioned_on AS site_commissioned_on,
        si.zone AS site_zone
    FROM profiled s
    LEFT JOIN sites si ON si.id = s.site_id
)
-- Subtotals with GROUPING SETS
SELECT
    zone,
    site_zone,
    site_id,
    SUM(CASE WHEN zone = 'Z4' THEN 1 ELSE 0 END) AS n_zone_z4
FROM joined_site
GROUP BY GROUPING SETS ((zone, site_zone, site_id), (zone), (site_zone))
ORDER BY zone NULLS LAST, site_zone NULLS LAST, site_id NULLS LAST
