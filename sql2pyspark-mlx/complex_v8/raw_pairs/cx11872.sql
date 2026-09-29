WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE SIZE(tags) >= 1
),
with_prev AS (
    -- Previous opened_at per priority
    SELECT *, LAG(opened_at) OVER (PARTITION BY priority ORDER BY opened_at, id) AS prev_opened_at
    FROM cleaned
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR (UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(prev_opened_at)) / 60 > 30 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY priority ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        priority,
        session_no,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(csat) AS session_csat,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY priority, session_no
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(session_no / NULLIF(SUM(session_no) OVER (PARTITION BY session_no, YEAR(session_end)), 0), 4) AS session_no_share_of_session_no_session_end_year,
        ROW_NUMBER() OVER w_session_start_year_session_n AS duration_min_seq
    FROM per_session
    WINDOW w_session_start_year_session_n AS (PARTITION BY YEAR(session_start), session_no ORDER BY duration_min, priority, session_no)
)
SELECT
    priority,
    session_no,
    session_start,
    session_end,
    event_count,
    session_csat,
    duration_min,
    session_no_share_of_session_no_session_end_year,
    duration_min_seq,
    ROUND((UNIX_TIMESTAMP(session_end) - UNIX_TIMESTAMP(session_start)) / 3600, 1) AS hours_session_start_to_session_end,
    LEAST(duration_min_seq, session_no) AS least_duration_min_seq_session_no
FROM with_trends
WHERE duration_min < 180.0
ORDER BY session_no, hours_session_start_to_session_end
