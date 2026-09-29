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
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(read_at) OVER (ORDER BY kwh, id) AS prev_read_at,
        DENSE_RANK() OVER (ORDER BY rate NULLS LAST, id) AS rate_dense_rank,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY quality, zone)) AS distinct_site_id_in_quality_zone,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY quality, zone), 0), 4) AS kwh_share_of_quality_zone
    FROM joined_ranges
),
joined_site AS (
    -- Join sites
    SELECT
        s.*,
        si.commissioned_on AS site_commissioned_on,
        si.installed_kw AS site_installed_kw
    FROM sequenced s
    LEFT JOIN sites si ON si.id = s.site_id AND si.zone = 'Z4'
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    quality,
    rate,
    energy_cost,
    prev_read_at,
    rate_dense_rank,
    distinct_site_id_in_quality_zone,
    site_commissioned_on,
    site_installed_kw,
    CASE WHEN zone IN ('Z4', 'Z3') THEN 'group_a' WHEN zone = 'Z2' THEN 'group_b' ELSE 'other' END AS zone_group,
    COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
FROM joined_site
ORDER BY kwh
