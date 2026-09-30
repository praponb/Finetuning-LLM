WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        DATE_FORMAT(read_at, 'yyyy-MM-dd') AS read_at_label,
        DATE_TRUNC('WEEK', read_at) AS read_at_week
    FROM meter_readings
    WHERE voltage >= 224.8
      AND quality <> 'BAD'
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN voltage >= 232.0 THEN 'A' WHEN voltage >= 224.9 THEN 'B' ELSE 'C' END AS voltage_tier,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM recent
),
exploded_detail AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.read_at,
        s.voltage,
        s.quality,
        s.read_at_week,
        s.quality_or_unknown,
        m.id AS reading_id,
        m.voltage AS reading_voltage,
        m.quality AS reading_quality,
        m.kwh AS reading_kwh
    FROM enriched s
    JOIN meter_readings m ON m.site_id = s.site_id
),
qualified AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM exploded_detail s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage < 232.7
              AND m.read_at >= TIMESTAMP '2025-02-06 11:00:00'
        )
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        read_at_week,
        quality_or_unknown,
        reading_id,
        SUBSTRING(TRIM(quality_or_unknown), 1, 2) AS quality_or_unknown_prefix,
        CASE WHEN voltage >= 233.8 THEN 'A' WHEN voltage >= 218.4 THEN 'B' ELSE 'C' END AS voltage_tier
    FROM qualified
    WHERE reading_voltage < 236.8
),
with_children_readings AS (
    -- One row per reading
    SELECT
        s.id,
        s.site_id,
        s.quality_or_unknown,
        s.reading_id,
        s.voltage_tier,
        m.id AS reading_id_2,
        m.read_at AS reading_read_at,
        m.voltage AS reading_voltage,
        m.kwh AS reading_kwh
    FROM scored s
    JOIN meter_readings m ON m.site_id = s.site_id AND m.voltage >= 218.2
)
-- Subtotals with GROUPING SETS
SELECT
    quality_or_unknown,
    site_id,
    COUNT(DISTINCT reading_id) AS distinct_reading_ids
FROM with_children_readings
GROUP BY GROUPING SETS ((quality_or_unknown, site_id), (quality_or_unknown), (site_id))
ORDER BY quality_or_unknown NULLS LAST, site_id NULLS LAST
