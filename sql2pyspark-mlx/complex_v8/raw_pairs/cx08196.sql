WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_name,
        zone,
        installed_kw,
        ROUND(installed_kw * 0.9, 2) AS installed_kw_adj,
        COALESCE(installed_kw, 0.0) AS installed_kw_filled,
        COALESCE(tariff, 'UNKNOWN') AS tariff_or_unknown
    FROM sites
    WHERE site_name IS NOT NULL
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT site_id, COUNT(*) AS reading_count
    FROM meter_readings
    GROUP BY site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count
    FROM recent s
    LEFT JOIN reading_stats m ON m.site_id = s.id
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        tariff_or_unknown,
        reading_count,
        zone,
        COUNT(*) AS cnt,
        COUNT(DISTINCT tariff_or_unknown) AS distinct_tariff_or_unknowns,
        ROUND(AVG(installed_kw_adj), 2) AS avg_installed_kw_adj,
        SUM(reading_count) AS total_reading_count,
        ARRAY_SORT(COLLECT_SET(zone)) AS zone_list
    FROM with_counts
    GROUP BY tariff_or_unknown, reading_count, zone
),
per_label AS (
    -- One row per zone_list_item
    SELECT
        s.tariff_or_unknown,
        s.reading_count,
        s.zone,
        s.cnt,
        s.distinct_tariff_or_unknowns,
        s.avg_installed_kw_adj,
        s.total_reading_count,
        t.zone_list_item
    FROM stats s
    LATERAL VIEW OUTER EXPLODE(s.zone_list) t AS zone_list_item
)
-- Final projection
SELECT
    tariff_or_unknown,
    reading_count,
    zone,
    cnt,
    distinct_tariff_or_unknowns,
    avg_installed_kw_adj,
    total_reading_count,
    zone_list_item
FROM per_label
ORDER BY total_reading_count
