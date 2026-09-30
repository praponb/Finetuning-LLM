WITH prepared AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        SUBSTRING(TRIM(quality), 1, 2) AS quality_prefix,
        LENGTH(TRIM(quality)) AS quality_len,
        LEAST(voltage, kwh) AS least_voltage_kwh
    FROM meter_readings
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2025-06-30', read_at) AS days_since_read_at,
        CASE WHEN quality_len >= 10 THEN 'A' WHEN quality_len >= 4 THEN 'B' ELSE 'C' END AS quality_len_tier,
        CONCAT_WS('|', quality_prefix, quality) AS quality_prefix_quality_label
    FROM prepared
    WHERE kwh >= 20.3
),
qualified AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM labeled s
    WHERE s.site_id IN (
            SELECT m.site_id
            FROM meter_readings m
        )
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        quality_prefix,
        least_voltage_kwh,
        days_since_read_at,
        quality_prefix_quality_label,
        YEAR(read_at) AS read_at_year,
        kwh + quality_len AS kwh_plus_quality_len
    FROM qualified
),
with_details AS (
    -- Join sites
    SELECT s.*, si.site_name, si.commissioned_on AS site_commissioned_on
    FROM enriched s
    JOIN sites si ON si.id = s.site_id
)
-- Subtotals with CUBE
SELECT
    site_id,
    COALESCE(quality, 'ALL') AS quality,
    read_at_year,
    GROUPING_ID() AS grouping_level,
    ROUND(STDDEV(days_since_read_at), 2) AS stddev_days_since_read_at
FROM with_details
GROUP BY CUBE(site_id, quality, read_at_year)
ORDER BY site_id NULLS LAST, quality NULLS LAST, read_at_year NULLS LAST
