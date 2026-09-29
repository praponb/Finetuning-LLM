WITH joined_ranges AS (
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
enriched_site AS (
    -- Attach site attributes
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.rate,
        s.energy_cost,
        si.tariff AS site_tariff,
        si.installed_kw AS site_installed_kw,
        si.commissioned_on AS site_commissioned_on
    FROM joined_ranges s
    JOIN sites si ON si.id = s.site_id
)
SELECT
    id,
    kwh,
    rate,
    energy_cost,
    site_tariff,
    site_commissioned_on,
    LEAST(site_installed_kw, kwh) AS least_site_installed_kw_kwh,
    UPPER(TRIM(site_tariff)) AS site_tariff_clean
FROM enriched_site
WHERE kwh BETWEEN 15.9 AND 96.8
ORDER BY site_tariff
