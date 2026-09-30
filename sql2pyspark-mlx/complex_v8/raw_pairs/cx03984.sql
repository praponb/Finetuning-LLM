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
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        si.installed_kw AS site_installed_kw,
        si.zone AS site_zone,
        si.tariff AS site_tariff
    FROM covered s
    JOIN sites si ON si.id = s.site_id
),
with_parent_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.rate,
        s.energy_cost,
        s.site_installed_kw,
        s.site_zone,
        s.site_tariff,
        si.site_name,
        si.tariff AS site_tariff_2
    FROM combined_details s
    JOIN sites si ON si.id = s.site_id
    WHERE si.tariff = 'EV'
),
with_details AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.voltage,
        s.energy_cost,
        s.site_installed_kw,
        s.site_zone,
        s.site_tariff,
        s.site_tariff_2,
        si.zone AS site_zone_2,
        si.tariff AS site_tariff_3
    FROM with_parent_site s
    JOIN sites si ON si.id = s.site_id
)
SELECT DISTINCT energy_cost, site_tariff_2
FROM with_details
ORDER BY energy_cost
