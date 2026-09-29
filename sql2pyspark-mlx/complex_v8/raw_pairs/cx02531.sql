WITH valid AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        DATEDIFF(DATE '2025-06-30', read_at) AS days_since_read_at
    FROM meter_readings
    WHERE kwh BETWEEN 37.1 AND 92.5
),
screened AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.kwh < 85.7
        )
),
latest_per_key AS (
    -- Keep the earliest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        days_since_read_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY kwh, id) AS rn
        FROM screened
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    DATE_TRUNC('WEEK', read_at) AS read_at_week,
    DATE_FORMAT(read_at, 'MMM yyyy') AS read_at_label
FROM latest_per_key
WHERE kwh > 52.5
ORDER BY id, kwh DESC
