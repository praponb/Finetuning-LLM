WITH recent AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        csat,
        SUBSTRING(TRIM(channel), 1, 2) AS channel_prefix
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'upgrade')
),
with_prev AS (
    -- Previous opened_at per agent_id
    SELECT *, LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY opened_at, id) AS prev_opened_at
    FROM recent
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY agent_id ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        agent_id,
        burst_id,
        MIN(opened_at) AS session_start,
        MAX(opened_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(requester_id) AS session_requester_id,
        ROUND((UNIX_TIMESTAMP(MAX(opened_at)) - UNIX_TIMESTAMP(MIN(opened_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY agent_id, burst_id
),
exploded_detail AS (
    -- One row per ticket
    SELECT
        s.agent_id,
        s.burst_id,
        s.duration_min,
        t.id AS ticket_id,
        t.requester_id AS ticket_requester_id,
        t.tags AS ticket_tags,
        t.channel AS ticket_channel,
        t.csat AS ticket_csat
    FROM per_session s
    JOIN tickets t ON t.agent_id = s.agent_id AND t.channel NOT IN ('email', 'web')
)
-- Final projection
SELECT
    agent_id,
    burst_id,
    duration_min,
    ticket_id,
    ticket_tags,
    ticket_csat,
    INITCAP(TRIM(ticket_channel)) AS ticket_channel_clean,
    SIZE(ticket_tags) AS ticket_tags_count
FROM exploded_detail
WHERE agent_id IS NOT NULL
ORDER BY agent_id NULLS LAST, burst_id, ticket_id
LIMIT 10
