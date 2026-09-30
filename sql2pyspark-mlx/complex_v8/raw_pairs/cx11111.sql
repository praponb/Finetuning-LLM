WITH joined_ranges AS (
    -- Events that happened while the ticket was open
    SELECT
        e.id,
        e.ticket_id,
        e.event_at,
        e.event_type,
        t.priority,
        t.agent_id,
        ROUND((UNIX_TIMESTAMP(e.event_at) - UNIX_TIMESTAMP(t.opened_at)) / 3600, 2) AS hours_since_open
    FROM ticket_events e
    JOIN tickets t
      ON t.id = e.ticket_id
     AND e.event_at >= t.opened_at
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 2 DAYS)
),
with_details AS (
    -- Join support_agents
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.agent_id,
        su.tier AS agent_tier,
        su.agent_name,
        t.channel AS ticket_channel
    FROM joined_ranges s
    JOIN support_agents su ON su.id = s.agent_id
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY ticket_channel ORDER BY event_at ASC) AS event_at_rank,
        LEAD(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id) AS next_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY ticket_channel ORDER BY event_at ASC, id))) / 60, 1) AS minutes_since_prev_event_at,
        LAST_VALUE(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        ROW_NUMBER() OVER (PARTITION BY ticket_channel ORDER BY event_at ASC, id) AS seq_no
    FROM with_details
    WINDOW w_ticket_channel_by_event_at AS (PARTITION BY ticket_channel ORDER BY event_at ASC, id)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    agent_id,
    agent_tier,
    ticket_channel,
    event_at_rank,
    next_event_at,
    minutes_since_prev_event_at,
    ROUND(event_at_rank / NULLIF(seq_no, 0), 3) AS event_at_rank_per_seq_no,
    COALESCE(ticket_channel, 'UNKNOWN') AS ticket_channel_or_unknown
FROM ranked
WHERE TO_DATE(next_event_at) < DATE '2025-01-31'
ORDER BY agent_tier DESC NULLS FIRST, id
LIMIT 10
