WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM tickets
),
with_prev AS (
    -- Previous opened_at per channel
    SELECT *, LAG(opened_at) OVER (PARTITION BY channel ORDER BY opened_at, id) AS prev_opened_at
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY channel ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        channel,
        burst_id,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY channel, burst_id
),
annotated AS (
    -- Label and score rows
    SELECT
        channel,
        burst_id,
        session_start,
        events,
        UPPER(TRIM(channel)) AS channel_clean,
        ROUND(duration_min / NULLIF(events, 0), 3) AS duration_min_per_events,
        HOUR(session_end) AS session_end_hour
    FROM per_session
    WHERE HOUR(session_end) BETWEEN 9 AND 12
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM annotated
    WHERE session_end_hour > (SELECT AVG(session_end_hour) FROM annotated)
)
SELECT *
FROM (
    SELECT session_end_hour, channel, events
    FROM qualified
) src
PIVOT (
    SUM(events)
    FOR channel IN ('email', 'phone', 'chat')
)
ORDER BY session_end_hour
