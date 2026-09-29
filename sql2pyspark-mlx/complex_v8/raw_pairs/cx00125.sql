WITH recent AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix,
        ROUND(kwh / NULLIF(voltage, 0), 3) AS kwh_per_voltage,
        DATEDIFF(DATE '2025-06-30', read_at) AS days_since_read_at
    FROM meter_readings
    WHERE site_id IS NOT NULL
      AND quality IN ('EST', 'OK')
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        site_id,
        kwh,
        quality,
        quality_prefix,
        COALESCE(kwh_per_voltage, 0.0) AS kwh_per_voltage_filled
    FROM recent
    WHERE quality_prefix IS NOT NULL
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM enriched
    WHERE kwh_per_voltage_filled > (SELECT MIN(kwh_per_voltage_filled) FROM enriched)
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN quality IN ('BAD', 'EST') THEN 'group_a' WHEN quality = 'OK' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM screened
),
screened_2 AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM derived s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh < 111.9
        )
)
-- Subtotals with GROUPING SETS
SELECT
    quality,
    quality_group,
    GROUPING_ID() AS gid,
    COUNT(*) AS row_count,
    ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
    ROUND(SUM(kwh_per_voltage_filled) / NULLIF(SUM(kwh), 0), 3) AS kwh_per_voltage_filled_to_kwh_ratio
FROM screened_2
GROUP BY GROUPING SETS ((quality, quality_group), (quality))
ORDER BY quality NULLS LAST, quality_group NULLS LAST
