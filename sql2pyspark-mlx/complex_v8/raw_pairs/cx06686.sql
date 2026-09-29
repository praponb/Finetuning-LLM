WITH filtered_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        channel,
        csat,
        SIZE(tags) AS tags_count
    FROM tickets
    WHERE agent_id <= 4
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM filtered_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR (UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(prev_opened_at)) / 60 > 90 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        agent_id,
        session_id,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(tags_count) AS session_tags_count,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY agent_id, session_id
)
-- Subtotals with GROUPING SETS
SELECT
    n_events,
    session_tags_count,
    session_id,
    MAX(session_start) AS last_session_start
FROM session_summary
GROUP BY GROUPING SETS ((n_events, session_tags_count, session_id), (n_events), ())
ORDER BY n_events NULLS LAST, session_tags_count NULLS LAST, session_id NULLS LAST
