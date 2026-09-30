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
    LEFT JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        si.zone AS site_zone,
        si.installed_kw AS site_installed_kw,
        si.site_name
    FROM joined_ranges s
    JOIN sites si ON si.id = s.site_id AND si.commissioned_on >= DATE '2024-08-24'
    WHERE si.site_name IS NOT NULL
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    rate,
    energy_cost,
    site_installed_kw,
    site_name,
    FLOOR(site_installed_kw / 100) * 100 AS site_installed_kw_bucket,
    LOWER(REGEXP_REPLACE(TRIM(site_zone), ' ', '_')) AS site_zone_slug
FROM combined_details
WHERE UPPER(TRIM(site_name)) LIKE 'S%'
ORDER BY rate
