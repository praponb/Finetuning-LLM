WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        zone,
        tariff,
        installed_kw,
        commissioned_on,
        CONCAT(tariff, '#', CAST(id AS STRING)) AS tariff_ref,
        CASE WHEN installed_kw >= 284.0 THEN 'HIGH' WHEN installed_kw >= 141.6 THEN 'MEDIUM' ELSE 'LOW' END AS installed_kw_tier
    FROM sites
    WHERE tariff = 'TOU'
      AND UPPER(TRIM(site_name)) LIKE 'R%'
),
totals AS (
    -- Group-level totals
    SELECT
        zone,
        COUNT(*) FILTER (WHERE LENGTH(TRIM(site_name)) >= 7) AS n_length_trim_site_name_7,
        COUNT(DISTINCT zone) AS distinct_zones,
        ARRAY_SORT(COLLECT_SET(site_name)) AS site_name_list,
        PERCENTILE_APPROX(installed_kw, 0.5) AS median_installed_kw,
        DATEDIFF(MAX(commissioned_on), MIN(commissioned_on)) AS commissioned_on_span_days
    FROM prepared
    GROUP BY zone
    HAVING MAX(installed_kw) > 93.3
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(commissioned_on_span_days - AVG(commissioned_on_span_days) OVER (PARTITION BY zone), 2) AS commissioned_on_span_days_vs_zone_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY distinct_zones, n_length_trim_site_name_7 ORDER BY n_length_trim_site_name_7 DESC, zone), 3) AS n_length_trim_site_name_7_cume_dist,
        ROUND(n_length_trim_site_name_7 / NULLIF(SUM(n_length_trim_site_name_7) OVER (PARTITION BY zone), 0), 4) AS n_length_trim_site_name_7_share_of_zone,
        MIN(commissioned_on_span_days) OVER (PARTITION BY zone) AS zone_min_commissioned_on_span_days
    FROM totals
),
scored AS (
    -- Label and score rows
    SELECT
        zone,
        n_length_trim_site_name_7,
        distinct_zones,
        site_name_list,
        commissioned_on_span_days,
        commissioned_on_span_days_vs_zone_avg,
        n_length_trim_site_name_7_cume_dist,
        n_length_trim_site_name_7_share_of_zone,
        ABS(distinct_zones - 6) AS distinct_zones_distance,
        SUBSTRING(TRIM(zone), 1, 3) AS zone_prefix
    FROM with_trends
    WHERE zone_min_commissioned_on_span_days < 4200
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        LENGTH(TRIM(zone)) AS zone_len,
        distinct_zones - distinct_zones_distance AS distinct_zones_minus_distinct_zones_distance
    FROM scored
    WHERE commissioned_on_span_days_vs_zone_avg >= -490.0
)
-- Final projection
SELECT
    zone,
    distinct_zones,
    site_name_list,
    commissioned_on_span_days,
    commissioned_on_span_days_vs_zone_avg,
    n_length_trim_site_name_7_cume_dist,
    distinct_zones_distance,
    zone_prefix,
    zone_len,
    distinct_zones_minus_distinct_zones_distance
FROM annotated
ORDER BY distinct_zones, zone_len DESC, zone
LIMIT 20
