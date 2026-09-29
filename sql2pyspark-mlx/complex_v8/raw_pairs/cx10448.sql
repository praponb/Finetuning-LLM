WITH base_readings AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        ROUND(voltage / NULLIF(kwh, 0), 3) AS voltage_per_kwh,
        DATEDIFF(DATE '2025-06-30', read_at) AS days_since_read_at,
        CONCAT(quality, '#', CAST(site_id AS STRING)) AS quality_ref
    FROM meter_readings
    WHERE site_id IS NOT NULL
),
enriched AS (
    -- Derive helper columns
    SELECT *, COALESCE(quality, 'UNKNOWN') AS quality_or_unknown
    FROM base_readings
),
latest AS (
    -- Keep the earliest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        voltage_per_kwh,
        days_since_read_at,
        quality_ref,
        quality_or_unknown
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY kwh, id) AS rn
        FROM enriched
    ) ranked
    WHERE rn = 1
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT site_id, read_at, quality, kwh
    FROM latest
) src
PIVOT (
    MIN(kwh)
    FOR quality IN ('EST', 'OK')
)
ORDER BY site_id, read_at
