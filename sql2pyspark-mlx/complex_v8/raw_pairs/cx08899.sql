WITH matched AS (
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
filtered AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM matched s
    WHERE EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND UPPER(TRIM(si.site_name)) LIKE 'R%'
              AND si.zone IS NOT NULL
        )
),
unique_rows AS (
    -- Keep the earliest row per site_id, zone
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        zone,
        rate,
        energy_cost
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id, zone ORDER BY rate, id) AS rn
        FROM filtered
    ) ranked
    WHERE rn = 1
),
combined_details_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.kwh,
        s.voltage,
        s.zone,
        s.rate,
        s.energy_cost,
        si.site_name,
        si.zone AS site_zone,
        si.installed_kw AS site_installed_kw
    FROM unique_rows s
    JOIN sites si ON si.id = s.site_id
    WHERE si.installed_kw >= 141.6
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    voltage,
    rate,
    energy_cost,
    site_name,
    site_zone,
    site_installed_kw,
    LOWER(REGEXP_REPLACE(TRIM(zone), ' ', '_')) AS zone_slug
FROM combined_details_site
ORDER BY site_installed_kw DESC, id
