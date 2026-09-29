WITH prepared AS (
    -- Filter sites
    SELECT
        id,
        site_name,
        zone,
        installed_kw,
        FLOOR(installed_kw / 100) * 100 AS installed_kw_bucket,
        CASE WHEN installed_kw >= 284.0 THEN 'A' WHEN installed_kw >= 141.6 THEN 'B' ELSE 'C' END AS installed_kw_tier
    FROM sites
    WHERE UPPER(TRIM(site_name)) LIKE 'H%'
),
reading_stats AS (
    -- Per-site meter_readings metrics
    SELECT
        site_id,
        COUNT(*) AS reading_count,
        COUNT(*) FILTER (WHERE quality IN ('OK', 'EST')) AS reading_quality_count
    FROM meter_readings
    GROUP BY site_id
),
with_counts AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        COALESCE(m.reading_count, 0) AS reading_count,
        COALESCE(m.reading_quality_count, 0) AS reading_quality_count
    FROM prepared s
    LEFT JOIN reading_stats m ON m.site_id = s.id
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(zone, 'UNKNOWN') AS zone_or_unknown,
        CASE WHEN installed_kw_tier IN ('C', 'A') THEN 'group_a' WHEN installed_kw_tier = 'B' THEN 'group_b' ELSE 'other' END AS installed_kw_tier_group
    FROM with_counts
    WHERE installed_kw_tier IS NOT NULL
),
with_stats AS (
    -- Attach meter_readings metrics
    SELECT
        s.*,
        (SELECT MIN(m.voltage) FROM meter_readings m WHERE m.site_id = s.id) AS min_reading_voltage
    FROM enriched s
)
SELECT *
FROM (
    SELECT reading_quality_count, zone, zone_or_unknown, min_reading_voltage
    FROM with_stats
) src
PIVOT (
    SUM(min_reading_voltage) AS amount, COUNT(min_reading_voltage) AS n
    FOR zone_or_unknown IN ('Z2', 'Z3', 'UNKNOWN')
)
