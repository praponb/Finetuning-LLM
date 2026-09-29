WITH base_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN event_type IN ('resolved', 'replied') THEN 'group_a' WHEN event_type = 'reopened' THEN 'group_b' ELSE 'other' END AS event_type_group,
        YEAR(event_at) AS event_at_year,
        SUBSTRING(TRIM(actor), 1, 3) AS actor_prefix
    FROM ticket_events
    WHERE MONTH(event_at) IN (1, 2)
      AND ticket_id IS NOT NULL
),
event_type_group_stats AS (
    -- Avg event_at_year per event_type_group
    SELECT event_type_group, AVG(event_at_year) AS avg_event_at_year
    FROM base_events
    GROUP BY event_type_group
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_event_at_year,
        ROUND(s.event_at_year - g.avg_event_at_year, 2) AS event_at_year_minus_avg
    FROM base_events s
    JOIN event_type_group_stats g ON g.event_type_group = s.event_type_group
    WHERE s.event_at_year > g.avg_event_at_year
),
joined AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.event_at_year,
        s.actor_prefix,
        s.avg_event_at_year,
        t.requester_id AS ticket_requester_id,
        t.closed_at AS ticket_closed_at,
        t.opened_at AS ticket_opened_at,
        su.agent_name
    FROM compared s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
)
-- Subtotals with GROUPING SETS
SELECT
    event_at_year,
    ticket_id,
    event_type,
    MIN(ticket_closed_at) AS first_ticket_closed_at,
    SUM(ticket_requester_id) AS total_ticket_requester_id
FROM joined
GROUP BY GROUPING SETS ((event_at_year, ticket_id, event_type), (event_at_year), (ticket_id))
ORDER BY event_at_year NULLS LAST, ticket_id NULLS LAST, event_type NULLS LAST
