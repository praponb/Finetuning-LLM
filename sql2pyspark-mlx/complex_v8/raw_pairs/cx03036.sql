WITH recent AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        SUBSTRING(TRIM(quality), 1, 3) AS quality_prefix,
        COALESCE(voltage, 0.0) AS voltage_filled
    FROM meter_readings
    WHERE quality IS NOT NULL
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        kwh,
        voltage,
        quality,
        voltage_filled,
        ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh,
        INITCAP(TRIM(quality_prefix)) AS quality_prefix_clean
    FROM recent
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        kwh,
        voltage,
        quality,
        voltage_per_kwh,
        quality_prefix_clean,
        FIRST_VALUE(voltage_per_kwh) IGNORE NULLS OVER (PARTITION BY quality ORDER BY voltage DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_voltage_per_kwh,
        DENSE_RANK() OVER w_quality_by_voltage AS voltage_dense_rank
    FROM derived
    WINDOW w_quality_by_voltage AS (PARTITION BY quality ORDER BY voltage DESC, id)
)
-- Final projection
SELECT
    id,
    kwh,
    voltage,
    quality,
    voltage_per_kwh,
    quality_prefix_clean,
    voltage_dense_rank
FROM ranked
ORDER BY voltage_per_kwh
