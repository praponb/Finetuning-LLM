WITH recent_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh
    FROM meter_readings
    WHERE voltage > 216.5
),
with_details AS (
    -- Enrich with dimension data
    SELECT s.*, si.tariff AS site_tariff
    FROM recent_readings s
    LEFT JOIN sites si ON si.id = s.site_id AND si.tariff = 'FLAT'
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        read_at,
        voltage,
        quality,
        site_tariff,
        COALESCE(voltage, 0.0) AS voltage_filled
    FROM with_details
    WHERE read_at >= TIMESTAMP '2025-01-19 12:00:00'
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        read_at,
        voltage,
        quality,
        site_tariff,
        COALESCE(voltage, 0.0) AS voltage_filled_2,
        voltage - voltage_filled AS voltage_minus_voltage_filled,
        LENGTH(TRIM(site_tariff)) AS site_tariff_len
    FROM enriched
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM derived
    WHERE voltage > (SELECT MIN(voltage) FROM derived)
)
SELECT
    id,
    voltage,
    quality,
    site_tariff,
    voltage_filled_2,
    voltage_minus_voltage_filled,
    site_tariff_len
FROM screened
WHERE quality <> 'OK'
ORDER BY site_tariff_len NULLS LAST
