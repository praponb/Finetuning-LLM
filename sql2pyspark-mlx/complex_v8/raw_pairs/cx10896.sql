WITH base_tickets AS (
    -- Scope the tickets rows
    SELECT *
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'login')
),
with_prev AS (
    -- Previous opened_at per channel
    SELECT *, LAG(opened_at) OVER (PARTITION BY channel ORDER BY opened_at, id) AS prev_opened_at
    FROM base_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR (UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(prev_opened_at)) / 60 > 90 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY channel ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        channel,
        session_no,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS events,
        SUM(requester_id) AS session_requester_id,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY channel, session_no
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(session_requester_id / 25) * 25 AS session_requester_id_bucket,
        COALESCE(channel, 'UNKNOWN') AS channel_or_unknown
    FROM per_session
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM derived
    WHERE duration_min < (SELECT MAX(duration_min) FROM derived)
)
SELECT
    channel,
    session_start,
    session_end,
    events,
    duration_min,
    session_requester_id_bucket,
    ABS(duration_min - 108.0) AS duration_min_distance,
    YEAR(session_end) AS session_end_year
FROM screened
ORDER BY duration_min_distance
