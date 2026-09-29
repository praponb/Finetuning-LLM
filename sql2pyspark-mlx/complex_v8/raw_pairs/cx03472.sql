WITH covered AS (
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
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality,
        s.zone,
        s.rate,
        s.energy_cost,
        si.zone AS site_zone,
        si.commissioned_on AS site_commissioned_on
    FROM covered s
    JOIN sites si ON si.id = s.site_id
    WHERE si.zone = 'Z2'
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    quality,
    rate,
    energy_cost,
    site_zone,
    site_commissioned_on,
    LOWER(TRIM(zone)) AS zone_clean
FROM joined
WHERE zone NOT IN ('Z2', 'Z4')
ORDER BY zone_clean
