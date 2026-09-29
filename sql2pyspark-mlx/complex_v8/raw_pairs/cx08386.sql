WITH scoped_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        INITCAP(TRIM(quality)) AS quality_clean
    FROM meter_readings
    WHERE quality IS NOT NULL
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM scoped_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 120 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
SELECT
    id,
    read_at,
    kwh,
    voltage,
    quality,
    prev_read_at,
    new_session_flag,
    session_no,
    CASE WHEN session_no >= 4 THEN 'A' WHEN session_no >= 3 THEN 'B' ELSE 'C' END AS session_no_tier
FROM sessionized
WHERE voltage >= 215.7
