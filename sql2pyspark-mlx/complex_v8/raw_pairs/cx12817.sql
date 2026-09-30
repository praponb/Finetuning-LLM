WITH cleaned AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 120 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        quality,
        session_id,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY quality, session_id
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM sessions
    WHERE duration_min > (SELECT MIN(duration_min) FROM sessions)
),
derived AS (
    -- Add calculated columns
    SELECT *, COALESCE(duration_min, 0.0) AS duration_min_filled
    FROM screened
    WHERE duration_min < 192.0
)
SELECT
    quality,
    events,
    ARRAY_SORT(COLLECT_SET(quality)) AS quality_list,
    COUNT(DISTINCT quality) AS distinct_qualitys
FROM derived
GROUP BY CUBE(quality, events)
ORDER BY quality NULLS LAST, events NULLS LAST
