WITH base_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        DATEDIFF(DATE '2025-06-30', read_at) AS days_since_read_at
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN voltage >= 233.1 THEN 'HIGH' WHEN voltage >= 220.7 THEN 'MEDIUM' ELSE 'LOW' END AS voltage_tier,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown,
        COALESCE(quality, 'UNKNOWN') AS quality_or_unknown_2
    FROM base_readings
    WHERE voltage <= 238.3
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        voltage_tier,
        quality_or_unknown,
        SUBSTRING(TRIM(voltage_tier), 1, 4) AS voltage_tier_prefix,
        DATEDIFF(DATE '2025-06-30', read_at) AS days_since_read_at_2
    FROM scored
    WHERE quality = 'OK'
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    voltage,
    quality,
    voltage_tier,
    quality_or_unknown,
    voltage_tier_prefix,
    DATE_TRUNC('QUARTER', read_at) AS read_at_quarter,
    ROUND(voltage * 1.1, 2) AS voltage_adj
FROM labeled
ORDER BY site_id DESC, id DESC
