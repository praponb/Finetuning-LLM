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
with_details_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.zone,
        s.rate,
        s.energy_cost,
        si.zone AS site_zone,
        si.installed_kw AS site_installed_kw
    FROM joined_ranges s
    LEFT JOIN sites si ON si.id = s.site_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        zone,
        rate,
        site_zone,
        site_installed_kw,
        ROUND(site_installed_kw / NULLIF(SUM(site_installed_kw) OVER w_site_id_site_zone, 0), 4) AS site_installed_kw_share_of_site_id_site_zone,
        LAST_VALUE(rate) OVER (PARTITION BY site_zone, site_id ORDER BY energy_cost DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_rate
    FROM with_details_site
    WINDOW w_site_id_site_zone AS (PARTITION BY site_id, site_zone)
),
with_parent AS (
    -- Join sites
    SELECT s.*, si.tariff AS site_tariff
    FROM ranked s
    JOIN sites si ON si.id = s.site_id
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    voltage,
    zone,
    site_zone,
    site_installed_kw,
    site_installed_kw_share_of_site_id_site_zone,
    last_rate,
    site_tariff,
    LOWER(REGEXP_REPLACE(TRIM(zone), ' ', '_')) AS zone_slug,
    LENGTH(TRIM(site_tariff)) AS site_tariff_len
FROM with_parent
ORDER BY site_tariff
