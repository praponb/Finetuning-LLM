WITH scoped_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        CONCAT_WS('-', channel, priority) AS channel_priority_label
    FROM tickets
    WHERE agent_id IS NOT NULL
),
with_prev AS (
    -- Previous opened_at per priority
    SELECT *, LAG(opened_at) OVER (PARTITION BY priority ORDER BY opened_at, id) AS prev_opened_at
    FROM scoped_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_opened_at IS NULL OR opened_at > prev_opened_at + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY priority ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        MAX(csat) AS max_ticket_csat,
        COUNT(*) AS ticket_count,
        MAX(requester_id) AS max_ticket_requester_id
    FROM tickets
    GROUP BY agent_id
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.max_ticket_csat,
        COALESCE(t.ticket_count, 0) AS ticket_count,
        t.max_ticket_requester_id
    FROM sessionized s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
)
SELECT
    id,
    requester_id,
    opened_at,
    priority,
    channel,
    csat,
    tags,
    channel_priority_label,
    prev_opened_at,
    starts_session,
    burst_id,
    max_ticket_csat,
    max_ticket_requester_id
FROM summarized_rows
ORDER BY id DESC NULLS FIRST, priority DESC
