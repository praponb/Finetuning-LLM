WITH scoped_readings AS (
    -- Scope the meter_readings rows
    SELECT
        id,
        site_id,
        read_at,
        kwh,
        voltage,
        quality,
        DATE_FORMAT(read_at, 'yyyy-MM') AS read_at_label
    FROM meter_readings
),
with_prev AS (
    -- Previous read_at per quality
    SELECT *, LAG(read_at) OVER (PARTITION BY quality ORDER BY read_at, id) AS prev_read_at
    FROM scoped_readings
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_read_at IS NULL OR (UNIX_TIMESTAMP(read_at) - UNIX_TIMESTAMP(prev_read_at)) / 60 > 30 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY quality ORDER BY read_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        quality,
        burst_id,
        MIN(read_at) AS session_start,
        MAX(read_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(read_at)) - UNIX_TIMESTAMP(MIN(read_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY quality, burst_id
),
scored AS (
    -- Label and score rows
    SELECT
        quality,
        burst_id,
        session_start,
        session_end,
        event_count,
        ROUND(duration_min * 1.07, 2) AS duration_min_adj,
        LENGTH(TRIM(quality)) AS quality_len
    FROM session_summary
    WHERE session_start < TIMESTAMP '2025-02-13 09:00:00'
)
-- Subtotals with CUBE
SELECT
    quality,
    event_count,
    burst_id,
    GROUPING(quality) AS is_total_quality,
    COUNT(*) FILTER (WHERE quality <> 'OK') AS n_quality_ok
FROM scored
GROUP BY CUBE(quality, event_count, burst_id)
ORDER BY quality NULLS LAST, event_count NULLS LAST, burst_id NULLS LAST
