WITH filtered_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        ROUND(kwh * 0.25, 2) AS kwh_adj,
        ABS(kwh - 68.1) AS kwh_distance
    FROM meter_readings
    WHERE voltage > 221.4
),
enriched AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.quality,
        s.kwh_adj,
        s.kwh_distance,
        si.commissioned_on AS site_commissioned_on
    FROM filtered_readings s
    LEFT JOIN sites si ON si.id = s.site_id AND LOWER(si.site_name) LIKE '%m %'
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(site_id) OVER (PARTITION BY quality, YEAR(site_commissioned_on))) AS distinct_site_id_in_quality_site_commissioned_on,
        LAG(kwh_adj, 3) OVER w_quality_site_id_by_kwh_dista AS prev3_kwh_adj,
        ROUND(kwh / NULLIF(SUM(kwh) OVER (PARTITION BY quality, YEAR(site_commissioned_on)), 0), 4) AS kwh_share_of_quality_site_commissioned_on_year,
        ROUND(kwh_distance - AVG(kwh_distance) OVER (PARTITION BY site_id), 2) AS kwh_distance_vs_site_id_avg,
        NTILE(5) OVER w_quality_site_id_by_kwh_dista AS kwh_distance_quintile,
        FIRST_VALUE(site_commissioned_on) OVER (PARTITION BY quality, site_id ORDER BY kwh_distance DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_site_commissioned_on,
        RANK() OVER (PARTITION BY quality ORDER BY kwh_adj DESC, id) AS kwh_adj_rank,
        LAG(kwh_adj) OVER (PARTITION BY quality ORDER BY kwh_adj DESC, id) AS prev_kwh_adj,
        MAX(kwh_distance) OVER (PARTITION BY site_id, MONTH(site_commissioned_on) ORDER BY site_commissioned_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_kwh_distance_so_far
    FROM enriched
    WINDOW w_quality_site_id_by_kwh_dista AS (PARTITION BY quality, site_id ORDER BY kwh_distance DESC, id)
)
-- Final projection
SELECT
    id,
    kwh,
    quality,
    kwh_adj,
    kwh_distance,
    site_commissioned_on,
    distinct_site_id_in_quality_site_commissioned_on,
    prev3_kwh_adj,
    kwh_share_of_quality_site_commissioned_on_year,
    kwh_distance_quintile,
    max_kwh_distance_so_far,
    SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix,
    DATEDIFF(DATE '2025-12-31', site_commissioned_on) AS days_since_site_commissioned_on
FROM windowed
WHERE prev_kwh_adj < 117.0
ORDER BY kwh_adj DESC, distinct_site_id_in_quality_site_commissioned_on
