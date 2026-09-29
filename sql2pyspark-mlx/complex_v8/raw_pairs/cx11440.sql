WITH prepared AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        channel
    FROM tickets
),
with_prev AS (
    -- Previous opened_at per channel
    SELECT *, LAG(opened_at) OVER (PARTITION BY channel ORDER BY opened_at, id) AS prev_opened_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR (UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(prev_opened_at)) / 60 > 45 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY channel ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sessions AS (
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
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(session_end) - UNIX_TIMESTAMP(session_start)) / 3600, 1) AS hours_session_start_to_session_end,
        CASE WHEN channel IN ('email', 'phone') THEN 'group_a' WHEN channel = 'web' THEN 'group_b' ELSE 'other' END AS channel_group
    FROM sessions
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(duration_min * 1.1, 2) AS duration_min_adj,
        FLOOR(duration_min / 50) * 50 AS duration_min_bucket
    FROM enriched
)
SELECT
    channel,
    burst_id,
    session_start,
    session_end,
    events,
    duration_min,
    hours_session_start_to_session_end,
    channel_group,
    duration_min_adj,
    FLOOR(duration_min_adj / 50) * 50 AS duration_min_adj_bucket
FROM derived
