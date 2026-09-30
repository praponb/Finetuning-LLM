WITH filtered_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage
    FROM meter_readings
    WHERE (voltage >= 215.6 OR kwh <= 81.3)
),
screened AS (
    -- Rows that have matching meter_readings
    SELECT s.*
    FROM filtered_readings s
    WHERE EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND MONTH(m.read_at) IN (1, 3)
              AND m.quality <> 'OK'
        )
      AND s.voltage > 219.6
),
latest_per_key AS (
    -- Keep the latest row per site_id
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY kwh DESC, id DESC) AS rn
        FROM screened
    ) ranked
    WHERE rn = 1
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    DATEDIFF(DATE '2025-12-31', read_at) AS days_since_read_at
FROM latest_per_key
ORDER BY voltage, kwh DESC NULLS FIRST
