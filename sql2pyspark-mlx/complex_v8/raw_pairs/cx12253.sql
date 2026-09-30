WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        CASE WHEN quality IN ('OK', 'BAD') THEN 'group_a' WHEN quality = 'EST' THEN 'group_b' ELSE 'other' END AS quality_group,
        voltage + kwh AS voltage_plus_kwh,
        LEAST(kwh, voltage) AS least_kwh_voltage
    FROM meter_readings
    WHERE kwh >= 42.1
),
qualified AS (
    -- Keep rows whose site matches
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sites si
            WHERE si.id = s.site_id
              AND si.site_name IS NOT NULL
        )
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        kwh,
        quality,
        quality_group,
        voltage_plus_kwh,
        least_kwh_voltage,
        LENGTH(TRIM(quality_group)) AS quality_group_len,
        CONCAT_WS('-', quality_group, quality) AS quality_group_quality_label,
        LENGTH(TRIM(quality)) AS quality_len
    FROM qualified
    WHERE site_id IS NOT NULL
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM enriched
    WHERE kwh > (SELECT AVG(kwh) FROM enriched)
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(quality_group, 'ALL') AS quality_group,
    quality_len,
    GROUPING(quality_group) AS is_total_quality_group,
    SUM(quality_group_len) AS total_quality_group_len,
    COUNT(*) AS row_count,
    SUM(least_kwh_voltage) AS total_least_kwh_voltage
FROM filtered
GROUP BY GROUPING SETS ((quality_group, quality_len), (quality_group), ())
ORDER BY quality_group NULLS LAST, quality_len NULLS LAST
