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
    LEFT JOIN tariff_windows tw
      ON tw.tariff = st.tariff
     AND r.read_at >= tw.valid_from
     AND r.read_at < tw.valid_to
),
joined AS (
    -- Enrich with dimension data
    SELECT s.*, si.installed_kw AS site_installed_kw, si.zone AS site_zone
    FROM covered s
    LEFT JOIN sites si ON si.id = s.site_id
),
quality_stats AS (
    -- Avg site_installed_kw per quality
    SELECT quality, AVG(site_installed_kw) AS avg_site_installed_kw
    FROM joined
    GROUP BY quality
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_site_installed_kw,
        ROUND(s.site_installed_kw - g.avg_site_installed_kw, 2) AS site_installed_kw_minus_avg
    FROM joined s
    JOIN quality_stats g ON g.quality = s.quality
    WHERE s.site_installed_kw > g.avg_site_installed_kw
)
-- Subtotals with CUBE
SELECT
    COALESCE(quality, 'ALL') AS quality,
    COALESCE(site_zone, 'ALL') AS site_zone,
    COALESCE(zone, 'ALL') AS zone,
    GROUPING_ID() AS grouping_level,
    ROUND(STDDEV(kwh), 2) AS stddev_kwh
FROM vs_group
GROUP BY CUBE(quality, site_zone, zone)
ORDER BY quality NULLS LAST, site_zone NULLS LAST, zone NULLS LAST
