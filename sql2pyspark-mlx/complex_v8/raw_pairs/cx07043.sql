WITH scoped AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE voltage > 216.8
),
all_events AS (
    -- Stack the branches
    SELECT site_id, id
    FROM scoped
    UNION ALL
    SELECT id, site_id
    FROM meter_readings
    WHERE quality <> 'EST'
      AND voltage > 218.5
)
SELECT site_id, COUNT(*) AS n_rows, SUM(id) AS total_id
FROM all_events
GROUP BY site_id
ORDER BY site_id
