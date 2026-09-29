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
zone_stats AS (
    -- Median kwh per zone
    SELECT zone, PERCENTILE_APPROX(kwh, 0.5) AS median_kwh
    FROM covered
    GROUP BY zone
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_kwh, ROUND(s.kwh - g.median_kwh, 2) AS kwh_minus_median
    FROM covered s
    JOIN zone_stats g ON g.zone = s.zone
    WHERE s.kwh >= g.median_kwh
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM compared s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
        )
),
combined_details AS (
    -- Join sites
    SELECT
        s.*,
        si.zone AS site_zone,
        si.tariff AS site_tariff,
        si.site_name
    FROM qualified s
    JOIN sites si ON si.id = s.site_id
    WHERE si.zone IN ('Z4', 'Z2')
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    zone,
    rate,
    energy_cost,
    median_kwh,
    kwh_minus_median,
    site_zone,
    site_tariff,
    site_name,
    UPPER(TRIM(site_zone)) AS site_zone_clean
FROM combined_details
WHERE site_id <= 2
ORDER BY site_zone NULLS LAST
