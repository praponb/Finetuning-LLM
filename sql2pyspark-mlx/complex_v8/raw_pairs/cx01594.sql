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
    LEFT JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
rollup_base AS (
    -- Summarise per group
    SELECT
        zone,
        MAX(read_at) AS last_read_at,
        SUM(CASE WHEN zone <> 'Z4' THEN voltage ELSE 0 END) AS voltage_zone_z4,
        COUNT(DISTINCT zone) AS distinct_zones
    FROM covered
    GROUP BY zone
)
-- Subtotals with GROUPING SETS
SELECT
    distinct_zones,
    zone,
    GROUPING_ID() AS grouping_level,
    SUM(CASE WHEN zone IS NOT NULL THEN 1 ELSE 0 END) AS n_zone_is_not_null
FROM rollup_base
GROUP BY GROUPING SETS ((distinct_zones, zone), (distinct_zones), (zone))
ORDER BY distinct_zones NULLS LAST, zone NULLS LAST
