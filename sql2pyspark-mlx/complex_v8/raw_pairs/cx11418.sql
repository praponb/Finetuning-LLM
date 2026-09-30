WITH valid_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ABS(voltage - 227.1) AS voltage_distance
    FROM meter_readings
    WHERE quality IN ('BAD', 'OK')
      AND kwh < 84.9
),
annotated AS (
    -- Derive helper columns
    SELECT *, LENGTH(TRIM(quality)) AS quality_len
    FROM valid_readings
    WHERE voltage_distance > 85.8
),
latest AS (
    -- Keep the latest row per quality, site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        voltage_distance,
        quality_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality, site_id ORDER BY read_at DESC, id DESC) AS rn
        FROM annotated
    ) ranked
    WHERE rn = 1
),
annotated_2 AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(voltage_distance - 85.8) AS voltage_distance_distance,
        LENGTH(TRIM(quality)) AS quality_len_2
    FROM latest
    WHERE voltage > 215.7
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(quality, 'ALL') AS quality,
    quality_len,
    quality_len_2,
    MIN(voltage_distance) AS min_voltage_distance,
    COUNT(*) AS cnt,
    COUNT(DISTINCT quality) AS distinct_qualitys
FROM annotated_2
GROUP BY GROUPING SETS ((quality, quality_len, quality_len_2), (quality), (quality_len))
ORDER BY quality NULLS LAST, quality_len NULLS LAST, quality_len_2 NULLS LAST
