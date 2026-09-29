WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        team,
        hired_on
    FROM support_agents
    WHERE team IN ('VIP', 'Tech', 'Onboarding')
),
with_children AS (
    -- One row per ticket
    SELECT
        s.id,
        s.hired_on,
        t.id AS ticket_id,
        t.priority AS ticket_priority,
        t.tags AS ticket_tags,
        t.channel AS ticket_channel,
        t.requester_id AS ticket_requester_id
    FROM cleaned s
    LEFT JOIN tickets t ON t.agent_id = s.id
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at
    FROM with_children s
),
ticket_channel_stats AS (
    -- Median ticket_requester_id per ticket_channel
    SELECT ticket_channel, PERCENTILE_APPROX(ticket_requester_id, 0.5) AS median_ticket_requester_id
    FROM with_counts
    GROUP BY ticket_channel
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_ticket_requester_id,
        ROUND(s.ticket_requester_id - g.median_ticket_requester_id, 2) AS ticket_requester_id_minus_median
    FROM with_counts s
    JOIN ticket_channel_stats g ON g.ticket_channel = s.ticket_channel
    WHERE s.ticket_requester_id >= g.median_ticket_requester_id
),
with_details AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.hired_on,
        s.ticket_id,
        s.ticket_priority,
        s.ticket_tags,
        s.ticket_channel,
        s.ticket_requester_id_minus_median,
        t.priority AS ticket_priority_2,
        t.opened_at AS ticket_opened_at
    FROM compared s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
with_activity AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM with_details s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.csat < 4
        )
),
qualified_2 AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM with_activity s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.actor IS NOT NULL
              AND t.event_type = 'created'
        )
)
SELECT
    id,
    hired_on,
    ticket_id,
    ticket_channel,
    ticket_requester_id_minus_median,
    ticket_priority_2
FROM qualified_2
WHERE ticket_requester_id_minus_median >= -12.5
ORDER BY ticket_requester_id_minus_median DESC, hired_on DESC NULLS FIRST
