WITH recent AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix
    FROM meter_readings
    WHERE HOUR(read_at) BETWEEN 8 AND 12
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        quality_prefix,
        voltage - kwh AS voltage_minus_kwh
    FROM recent
),
screened AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.quality IS NOT NULL
              AND m.voltage < 229.1
        )
      AND s.quality IN ('BAD', 'EST')
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        ROUND(voltage * 1.07, 2) AS voltage_adj,
        CASE WHEN voltage >= 236.7 THEN 'A' WHEN voltage >= 222.3 THEN 'B' ELSE 'C' END AS voltage_tier
    FROM screened
    WHERE quality = 'BAD'
)
-- Subtotals with ROLLUP
SELECT
    site_id,
    voltage_tier,
    GROUPING(site_id) AS is_total_site_id,
    COUNT(DISTINCT site_id) AS distinct_site_ids,
    COUNT(*) AS cnt
FROM labeled
GROUP BY ROLLUP(site_id, voltage_tier)
ORDER BY site_id NULLS LAST, voltage_tier NULLS LAST
