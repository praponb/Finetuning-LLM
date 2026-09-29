WITH base AS (
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
    WHERE TO_DATE(opened_at) < DATE '2025-02-06'
),
with_prev AS (
    -- Previous opened_at per channel
    SELECT *, LAG(opened_at) OVER (PARTITION BY channel ORDER BY opened_at, id) AS prev_opened_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR (UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(prev_opened_at)) / 60 > 30 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY channel ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        channel,
        session_id,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS events,
        SUM(csat) AS session_csat,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY channel, session_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        channel,
        session_id,
        session_start,
        events,
        session_csat,
        duration_min,
        DENSE_RANK() OVER (PARTITION BY events ORDER BY events) AS events_dense_rank,
        ROUND(AVG(session_id) OVER (PARTITION BY events, session_id), 2) AS events_session_id_avg_session_id,
        RANK() OVER (PARTITION BY events ORDER BY events, channel, session_id) AS events_rank,
        MAX(session_csat) OVER (PARTITION BY events ORDER BY session_start ASC, channel, session_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_session_csat_so_far,
        COUNT(*) OVER (PARTITION BY events, session_id) AS rows_in_events_session_id,
        ROUND(events / NULLIF(SUM(events) OVER w_events_session_id, 0), 4) AS events_share_of_events_session_id
    FROM sessions
    WINDOW w_events_session_id AS (PARTITION BY events, session_id)
),
derived AS (
    -- Label and score rows
    SELECT
        channel,
        session_id,
        duration_min,
        events_dense_rank,
        events_share_of_events_session_id,
        COALESCE(session_id, 0) AS session_id_filled
    FROM ranked
    WHERE channel IN ('web', 'chat', 'phone')
)
-- Final projection
SELECT
    channel,
    session_id,
    duration_min,
    events_dense_rank,
    events_share_of_events_session_id,
    session_id_filled
FROM derived
WHERE events_dense_rank <= 8
ORDER BY channel DESC, duration_min DESC NULLS FIRST, session_id
LIMIT 10
