WITH cleaned_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        UPPER(TRIM(quality)) AS quality_clean
    FROM meter_readings
    WHERE kwh < 81.9
),
scored AS (
    -- Derive helper columns
    SELECT *, ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh
    FROM cleaned_readings
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(quality, 'ALL') AS quality,
    site_id,
    ROUND(SUM(voltage_per_kwh) / NULLIF(SUM(voltage), 0), 3) AS voltage_per_kwh_to_voltage_ratio,
    COUNT(*) AS n_rows,
    MAX(read_at) AS last_read_at
FROM scored
GROUP BY GROUPING SETS ((quality, site_id), (quality), (site_id), ())
ORDER BY quality NULLS LAST, site_id NULLS LAST
