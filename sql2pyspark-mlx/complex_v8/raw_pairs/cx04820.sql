WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
    WHERE kwh BETWEEN 20.4 AND 82.9
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 45 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
labeled AS (
    -- Label and score rows
    SELECT *, prev_read_at + INTERVAL 15 MINUTES AS prev_read_at_plus_15m
    FROM sessionized
    WHERE prev_read_at >= TIMESTAMP '2025-02-06 11:00:00'
),
qualified AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM labeled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.voltage >= 223.0
        )
      AND s.kwh >= 14.5
)
-- Final projection
SELECT
    id,
    site_id,
    read_at,
    voltage,
    prev_read_at,
    session_id,
    CASE WHEN is_new_session >= 1 THEN 'HIGH' WHEN is_new_session >= 0 THEN 'MEDIUM' ELSE 'LOW' END AS is_new_session_tier
FROM qualified
ORDER BY session_id DESC, id
LIMIT 5
