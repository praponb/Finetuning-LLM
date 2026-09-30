WITH in_window AS (
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
    LEFT JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
combined_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality,
        s.zone,
        s.rate,
        s.energy_cost,
        si.tariff AS site_tariff
    FROM in_window s
    JOIN sites si ON si.id = s.site_id
    WHERE si.tariff = 'TOU'
),
joined_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.quality,
        s.zone,
        s.rate,
        s.energy_cost,
        s.site_tariff,
        si.zone AS site_zone,
        si.site_name,
        si.commissioned_on AS site_commissioned_on
    FROM combined_details_site s
    LEFT JOIN sites si ON si.id = s.site_id AND si.installed_kw BETWEEN 66.5 AND 386.6
)
-- Final projection
SELECT
    id,
    quality,
    rate,
    energy_cost,
    site_tariff,
    site_name,
    ROUND(rate / NULLIF(energy_cost, 0), 3) AS rate_per_energy_cost,
    CONCAT(site_zone, '#', CAST(id AS STRING)) AS site_zone_ref
FROM joined_site
ORDER BY energy_cost DESC
