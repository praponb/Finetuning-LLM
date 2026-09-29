WITH scoped AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_TRUNC('YEAR', read_at) AS read_at_year,
        CONCAT(quality, '#', CAST(id AS STRING)) AS quality_ref
    FROM meter_readings
    WHERE voltage <= 239.6
),
deduped AS (
    -- Keep the latest row per site_id, quality
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        read_at_year,
        quality_ref
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id, quality ORDER BY read_at_year DESC, id DESC) AS recency_rank
        FROM scoped
    ) ranked
    WHERE recency_rank = 1
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_TRUNC('WEEK', read_at) AS read_at_week
    FROM deduped
    WHERE kwh < 90.6
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT read_at, quality, voltage
    FROM labeled
) src
PIVOT (
    MAX(voltage)
    FOR quality IN ('OK', 'BAD')
)
ORDER BY read_at
