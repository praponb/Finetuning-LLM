WITH filtered AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        HOUR(read_at) AS read_at_hour
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 30 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM sessionized
    WHERE read_at_hour < (SELECT MAX(read_at_hour) FROM sessionized)
)
-- Final projection
SELECT
    id,
    kwh,
    voltage,
    quality,
    read_at_hour,
    prev_read_at,
    is_new_session,
    burst_id,
    INITCAP(TRIM(quality)) AS quality_clean
FROM screened
ORDER BY quality DESC, quality_clean
