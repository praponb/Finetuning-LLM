WITH cleaned_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        SUBSTRING(TRIM(quality), 1, 4) AS quality_prefix,
        COALESCE(voltage, 0.0) AS voltage_filled
    FROM meter_readings
    WHERE voltage > 216.8
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(voltage_filled * 0.25, 2) AS voltage_filled_adj,
        CASE WHEN quality IN ('OK', 'EST') THEN 'group_a' WHEN quality = 'BAD' THEN 'group_b' ELSE 'other' END AS quality_group
    FROM cleaned_readings
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        site_id,
        voltage,
        quality_prefix,
        voltage_filled,
        MIN(kwh) OVER (PARTITION BY quality) AS quality_min_kwh,
        LAST_VALUE(voltage_filled) IGNORE NULLS OVER (PARTITION BY quality_group, quality ORDER BY voltage_filled_adj DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_voltage_filled,
        COUNT(*) OVER (PARTITION BY quality) AS rows_in_quality,
        RANK() OVER (PARTITION BY quality_group, quality ORDER BY voltage_filled_adj DESC) AS voltage_filled_adj_rank
    FROM scored
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, reading
FROM (
    SELECT id, rows_in_quality, voltage_filled_adj_rank
    FROM sequenced
) src
UNPIVOT INCLUDE NULLS (
    reading FOR field IN (rows_in_quality, voltage_filled_adj_rank)
) unp
ORDER BY id, field
