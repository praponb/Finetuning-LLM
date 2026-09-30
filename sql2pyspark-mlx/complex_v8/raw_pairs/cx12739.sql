WITH prepared_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE (event_type <> 'resolved' OR ticket_id IS NOT NULL)
),
with_details_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        t.csat AS ticket_csat,
        t.agent_id AS ticket_agent_id,
        t.channel AS ticket_channel
    FROM prepared_events s
    JOIN tickets t ON t.id = s.ticket_id AND t.requester_id BETWEEN 3 AND 18
),
ticket_id_stats AS (
    -- Max ticket_csat per ticket_id
    SELECT ticket_id, MAX(ticket_csat) AS max_ticket_csat
    FROM with_details_ticket
    GROUP BY ticket_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_ticket_csat,
        ROUND(s.ticket_csat - g.max_ticket_csat, 2) AS ticket_csat_minus_max
    FROM with_details_ticket s
    JOIN ticket_id_stats g ON g.ticket_id = s.ticket_id
    WHERE s.ticket_csat = g.max_ticket_csat
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT ticket_agent_id, ticket_id, ticket_channel, max_ticket_csat
    FROM compared
) src
PIVOT (
    MIN(max_ticket_csat)
    FOR ticket_channel IN ('email', 'phone')
)
