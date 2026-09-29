WITH filtered_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        LEAST(voltage, kwh) AS least_voltage_kwh
    FROM meter_readings
    WHERE voltage < 233.1
      AND HOUR(read_at) BETWEEN 9 AND 12
),
deduped AS (
    -- Keep the latest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        least_voltage_kwh
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY kwh DESC, id DESC) AS rn
        FROM filtered_readings
    ) ranked
    WHERE rn = 1
)
-- Subtotals with GROUPING SETS
SELECT
    quality,
    site_id,
    GROUPING(quality) AS is_total_quality,
    MAX(kwh) AS max_kwh,
    MIN(voltage) AS min_voltage,
    ROUND(SUM(voltage) / NULLIF(SUM(least_voltage_kwh), 0), 3) AS voltage_to_least_voltage_kwh_ratio
FROM deduped
GROUP BY GROUPING SETS ((quality, site_id), (quality), (site_id))
ORDER BY quality NULLS LAST, site_id NULLS LAST
