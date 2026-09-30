WITH cleaned_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        channel,
        tags,
        ABS(requester_id - 8) AS requester_id_distance
    FROM tickets
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(requester_id, 0) AS requester_id_filled,
        COALESCE(requester_id, 0) AS requester_id_filled_2,
        CONCAT(channel, '#', CAST(requester_id_distance AS STRING)) AS channel_ref
    FROM cleaned_tickets
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        COUNT(CASE WHEN channel = 'email' THEN 1 END) AS ticket_channel_count,
        COUNT(CASE WHEN priority <> 'P4' THEN 1 END) AS ticket_priority_count,
        MAX(closed_at) AS last_ticket_closed_at
    FROM tickets
    GROUP BY agent_id
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_channel_count, 0) AS ticket_channel_count,
        COALESCE(t.ticket_priority_count, 0) AS ticket_priority_count,
        t.last_ticket_closed_at
    FROM labeled s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
)
SELECT
    ticket_priority_count,
    agent_id,
    ROUND(AVG(ticket_channel_count), 2) AS avg_ticket_channel_count,
    COUNT(*) FILTER (WHERE channel IN ('web', 'email')) AS n_channel_in_web_email
FROM with_stats
GROUP BY ROLLUP(ticket_priority_count, agent_id)
ORDER BY ticket_priority_count NULLS LAST, agent_id NULLS LAST
