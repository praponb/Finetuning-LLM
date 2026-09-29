WITH joined_ranges AS (
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
with_details_site AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality,
        s.zone,
        s.rate,
        s.energy_cost,
        si.installed_kw AS site_installed_kw
    FROM joined_ranges s
    JOIN sites si ON si.id = s.site_id
)
SELECT
    zone,
    quality,
    site_id,
    COUNT(*) AS row_count,
    ROUND(SUM(rate), 2) AS total_rate,
    ROUND(SUM(kwh) / NULLIF(SUM(rate), 0), 3) AS kwh_to_rate_ratio
FROM with_details_site
GROUP BY CUBE(zone, quality, site_id)
ORDER BY zone NULLS LAST, quality NULLS LAST, site_id NULLS LAST
