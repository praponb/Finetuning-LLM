WITH cleaned_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
    WHERE TO_DATE(read_at) < DATE '2025-03-08'
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh,
        FLOOR(voltage / 5) * 5 AS voltage_bucket
    FROM cleaned_readings
    WHERE quality <> 'EST'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        site_id,
        kwh,
        voltage,
        quality_len,
        voltage_per_kwh,
        voltage_bucket,
        LAG(kwh, 1, 0) OVER (PARTITION BY site_id, quality ORDER BY voltage_bucket DESC, id) AS prev_kwh,
        MIN(voltage_bucket) OVER (PARTITION BY quality_len) AS quality_len_min_voltage_bucket,
        SUM(voltage_bucket) OVER (PARTITION BY site_id, quality ORDER BY voltage_bucket DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS voltage_bucket_sum_7,
        voltage_per_kwh - LAG(voltage_per_kwh) OVER w_site_id_quality_by_voltage_b AS voltage_per_kwh_change
    FROM tagged
    WINDOW w_site_id_quality_by_voltage_b AS (PARTITION BY site_id, quality ORDER BY voltage_bucket DESC, id)
)
-- Final projection
SELECT
    id,
    site_id,
    kwh,
    quality_len,
    voltage_per_kwh,
    voltage_bucket,
    prev_kwh,
    quality_len_min_voltage_bucket,
    voltage_bucket_sum_7
FROM with_windows
ORDER BY voltage_bucket_sum_7, site_id DESC NULLS FIRST
