WITH scoped AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        csat,
        tags,
        COALESCE(channel, 'UNKNOWN') AS channel_or_unknown
    FROM tickets
    WHERE csat >= 2
),
with_prev AS (
    -- Previous closed_at per agent_id
    SELECT *, LAG(closed_at) OVER (PARTITION BY agent_id ORDER BY closed_at, id) AS prev_closed_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_closed_at IS NULL OR closed_at > prev_closed_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY agent_id ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY requester_id, id) AS rn,
        SUM(requester_id) OVER (ORDER BY requester_id, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS requester_id_sum_7,
        ROUND(starts_session - AVG(starts_session) OVER w_burst_id, 2) AS starts_session_vs_burst_id_avg
    FROM sessionized
    WINDOW w_burst_id AS (PARTITION BY burst_id)
)
SELECT
    id,
    requester_id,
    agent_id,
    closed_at,
    csat,
    tags,
    channel_or_unknown,
    prev_closed_at,
    starts_session,
    burst_id,
    starts_session_vs_burst_id_avg,
    COALESCE(starts_session, 0) AS starts_session_filled,
    ROUND((UNIX_TIMESTAMP(prev_closed_at) - UNIX_TIMESTAMP(closed_at)) / 3600, 1) AS hours_closed_at_to_prev_closed_at
FROM with_windows
