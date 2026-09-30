WITH valid_tickets AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        closed_at,
        channel,
        csat,
        CONCAT_WS('|', priority, channel) AS priority_channel_label,
        ROUND(csat / NULLIF(requester_id, 0), 3) AS csat_per_requester_id
    FROM tickets
    WHERE requester_id > 6
      AND priority IN ('P2', 'P4', 'P3')
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, MAX(event_at) AS last_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at
    FROM valid_tickets s
    LEFT JOIN event_stats t ON t.ticket_id = s.id
),
enriched AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.channel,
        s.csat,
        s.csat_per_requester_id,
        su.agent_name,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team
    FROM with_counts s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.team = 'VIP'
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS last_event_at
    FROM enriched s
)
-- Final projection
SELECT
    id,
    agent_id,
    closed_at,
    channel,
    csat,
    csat_per_requester_id,
    agent_name,
    agent_team,
    last_event_at
FROM with_stats
ORDER BY csat, id
LIMIT 10
