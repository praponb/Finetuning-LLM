WITH scoped AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        DATEDIFF(DATE '2025-06-30', read_at) AS days_since_read_at
    FROM meter_readings
    WHERE kwh >= 35.9
),
deduped AS (
    -- Keep the latest row per quality, site_id
    SELECT
        id,
        site_id,
        read_at,
        voltage,
        quality,
        days_since_read_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY quality, site_id ORDER BY voltage DESC, id DESC) AS recency_rank
        FROM scoped
    ) ranked
    WHERE recency_rank = 1
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT site_id, quality, days_since_read_at
    FROM deduped
) src
PIVOT (
    COUNT(days_since_read_at)
    FOR quality IN ('OK', 'EST', 'BAD')
)
