WITH valid AS (
    -- Scope the meter_readings rows
    SELECT *
    FROM meter_readings
    WHERE read_at >= TIMESTAMP '2025-01-31 12:00:00'
)
SELECT site_id AS entity_id, quality AS name
FROM valid
WHERE site_id >= 2
UNION ALL
SELECT id AS entity_id, zone AS name
FROM sites
