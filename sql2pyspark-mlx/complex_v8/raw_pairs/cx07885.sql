WITH valid_sites AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        zone,
        tariff,
        commissioned_on,
        LOWER(REGEXP_REPLACE(TRIM(zone), ' ', '_')) AS zone_slug,
        CASE WHEN installed_kw >= 373.3 THEN 'HIGH' WHEN installed_kw >= 141.6 THEN 'MEDIUM' ELSE 'LOW' END AS installed_kw_tier,
        COALESCE(zone, 'UNKNOWN') AS zone_or_unknown
    FROM sites
    WHERE tariff IN ('TOU', 'EV')
      AND UPPER(TRIM(site_name)) LIKE 'S%'
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_name,
        tariff,
        commissioned_on,
        zone_slug,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(commissioned_on), zone_or_unknown ORDER BY commissioned_on DESC), 3) AS commissioned_on_cume_dist,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(commissioned_on), zone_or_unknown ORDER BY commissioned_on DESC, id), 3) AS commissioned_on_cume_dist_2
    FROM valid_sites
),
summarized_rows AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN m.quality = 'EST' THEN 1 END) FROM meter_readings m WHERE m.site_id = s.id) AS reading_quality_count
    FROM with_trends s
)
SELECT
    COALESCE(tariff, 'ALL') AS tariff,
    reading_quality_count,
    GROUPING(tariff) AS is_total_tariff,
    SUM(commissioned_on_cume_dist) AS total_commissioned_on_cume_dist,
    COUNT(DISTINCT id) AS distinct_ids,
    MIN(commissioned_on) AS first_commissioned_on
FROM summarized_rows
GROUP BY CUBE(tariff, reading_quality_count)
ORDER BY tariff NULLS LAST, reading_quality_count NULLS LAST
