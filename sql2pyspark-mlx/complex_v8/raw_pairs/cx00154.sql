WITH valid AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_TRUNC('QUARTER', read_at) AS read_at_quarter
    FROM meter_readings
    WHERE voltage < 235.0
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 90 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
screened AS (
    -- Rows with no matching meter_readings
    SELECT s.*
    FROM sessionized s
    WHERE NOT EXISTS (
            SELECT 1
            FROM meter_readings m
            WHERE m.site_id = s.site_id
              AND m.read_at < TIMESTAMP '2025-02-18 12:00:00'
              AND m.voltage > 223.3
        )
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT is_new_session, quality, kwh
    FROM screened
) src
PIVOT (
    AVG(kwh)
    FOR quality IN ('OK', 'BAD')
)
