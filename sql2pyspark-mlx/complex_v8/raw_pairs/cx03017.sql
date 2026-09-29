WITH recent AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        quality,
        LENGTH(TRIM(quality)) AS quality_len
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM recent
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_read_at IS NULL OR read_at > prev_read_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
-- Subtotals with GROUPING SETS
SELECT
    quality,
    starts_session,
    session_no,
    GROUPING_ID() AS gid,
    ROUND(STDDEV(quality_len), 2) AS stddev_quality_len,
    MIN(quality_len) AS min_quality_len
FROM sessionized
GROUP BY GROUPING SETS ((quality, starts_session, session_no), (quality), ())
ORDER BY quality NULLS LAST, starts_session NULLS LAST, session_no NULLS LAST
