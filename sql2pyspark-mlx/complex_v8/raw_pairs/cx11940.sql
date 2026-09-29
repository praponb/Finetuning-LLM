WITH prepared_readings AS (
    -- Filter meter_readings
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_TRUNC('DAY', read_at) AS read_at_day
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per site_id
    SELECT *, LAG(read_at) OVER (PARTITION BY site_id ORDER BY read_at, id) AS prev_read_at
    FROM prepared_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY site_id ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
SELECT
    id,
    site_id,
    read_at,
    kwh,
    voltage,
    quality,
    starts_session,
    session_no,
    ROUND((UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 3600, 1) AS hours_prev_read_at_to_read_at,
    LENGTH(TRIM(quality)) AS quality_len
FROM sessionized
WHERE starts_session BETWEEN 0 AND 1
ORDER BY read_at
