WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality
    FROM meter_readings
    WHERE quality IS NOT NULL
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        quality,
        LEAD(voltage) OVER (ORDER BY voltage, id) AS next_voltage,
        COUNT(*) OVER (PARTITION BY quality) AS rows_in_quality,
        ROW_NUMBER() OVER (PARTITION BY quality, site_id ORDER BY voltage, id) AS voltage_seq
    FROM base
),
first_rows AS (
    -- Top 3 per group
    SELECT
        id,
        quality,
        next_voltage,
        rows_in_quality
    FROM with_trends
    WHERE voltage_seq <= 3
      AND rows_in_quality >= 9
)
SELECT
    id,
    quality,
    next_voltage,
    rows_in_quality
FROM first_rows
ORDER BY quality, id
LIMIT 20
