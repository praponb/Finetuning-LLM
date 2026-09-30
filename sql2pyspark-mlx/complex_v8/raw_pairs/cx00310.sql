WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        COALESCE(kwh, 0.0) AS kwh_filled,
        ROUND(kwh * 1.07, 2) AS kwh_adj
    FROM meter_readings
    WHERE quality IN ('EST', 'OK')
),
combined_details_site AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality,
        s.kwh_filled,
        s.kwh_adj,
        si.zone AS site_zone,
        si.installed_kw AS site_installed_kw
    FROM filtered s
    JOIN sites si ON si.id = s.site_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(kwh_adj) OVER (ORDER BY kwh DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS kwh_adj_avg_3,
        FIRST_VALUE(kwh) IGNORE NULLS OVER (PARTITION BY site_id ORDER BY site_installed_kw ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_kwh,
        ROUND(kwh_filled / NULLIF(SUM(kwh_filled) OVER (PARTITION BY site_zone, site_id), 0), 4) AS kwh_filled_share_of_site_zone_site_id,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_zone, site_id)) AS distinct_quality_in_site_zone_site_id,
        COUNT(*) OVER (PARTITION BY site_id) AS rows_in_site_id,
        SUM(kwh_adj) OVER (ORDER BY kwh DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_kwh_adj,
        LAG(kwh_filled, 2) OVER (ORDER BY site_installed_kw DESC, id) AS prev2_kwh_filled,
        SIZE(COLLECT_SET(quality) OVER (PARTITION BY site_id)) AS distinct_quality_in_site_id
    FROM combined_details_site
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT_WS('-', quality, site_zone) AS quality_site_zone_label,
        COALESCE(site_zone, 'UNKNOWN') AS site_zone_or_unknown,
        FLOOR(site_installed_kw / 100) * 100 AS site_installed_kw_bucket
    FROM windowed
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(site_installed_kw_bucket) IGNORE NULLS OVER (ORDER BY kwh_adj_avg_3 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_site_installed_kw_bucket,
        ROUND(CUME_DIST() OVER (PARTITION BY quality, distinct_quality_in_site_zone_site_id ORDER BY site_installed_kw_bucket ASC), 3) AS site_installed_kw_bucket_cume_dist,
        ROUND(kwh - AVG(kwh) OVER (PARTITION BY site_zone, distinct_quality_in_site_id), 2) AS kwh_vs_site_zone_distinct_quality_in_site_id_avg,
        COUNT(*) OVER (PARTITION BY site_zone, distinct_quality_in_site_id) AS rows_in_site_zone_distinct_quality_in_site_id
    FROM tagged
)
SELECT
    id,
    kwh,
    kwh_filled,
    kwh_adj,
    site_installed_kw,
    kwh_adj_avg_3,
    first_known_kwh,
    distinct_quality_in_site_zone_site_id,
    rows_in_site_id,
    running_kwh_adj,
    prev2_kwh_filled,
    distinct_quality_in_site_id,
    quality_site_zone_label,
    site_installed_kw_bucket,
    last_known_site_installed_kw_bucket,
    site_installed_kw_bucket_cume_dist,
    kwh_vs_site_zone_distinct_quality_in_site_id_avg,
    rows_in_site_zone_distinct_quality_in_site_id,
    COALESCE(site_zone, 'UNKNOWN') AS site_zone_or_unknown_2
FROM sequenced
ORDER BY site_installed_kw, id DESC NULLS FIRST
LIMIT 20
