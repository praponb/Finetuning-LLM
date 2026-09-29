WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_FORMAT(read_at, 'yyyy-MM-dd') AS read_at_label,
        INITCAP(TRIM(quality)) AS quality_clean,
        ABS(kwh - 74.3) AS kwh_distance
    FROM meter_readings
    WHERE quality IN ('BAD', 'EST')
      AND MONTH(read_at) IN (1, 3)
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        quality,
        read_at_label,
        quality_clean,
        ABS(kwh_distance - 78.0) AS kwh_distance_distance
    FROM filtered
    WHERE voltage < 238.3
),
joined AS (
    -- Join sites
    SELECT
        s.id,
        s.site_id,
        s.kwh,
        s.read_at_label,
        s.quality_clean,
        s.kwh_distance_distance,
        si.zone AS site_zone,
        si.site_name
    FROM tagged s
    LEFT JOIN sites si ON si.id = s.site_id
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(CASE WHEN quality <> 'EST' THEN 1 END) AS reading_quality_count
    FROM meter_readings
    GROUP BY site_id
),
profiled AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count
    FROM joined s
    LEFT JOIN reading_stats m ON m.site_id = s.site_id
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    kwh_distance_distance,
    site_name,
    reading_quality_count,
    UPPER(TRIM(site_zone)) AS site_zone_clean
FROM profiled
ORDER BY kwh_distance_distance, kwh DESC
