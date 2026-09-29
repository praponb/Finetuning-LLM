WITH cleaned AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at_2
    FROM ticket_events
    WHERE actor <> 'agent'
      AND ticket_id >= 29
),
joined_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        t.requester_id AS ticket_requester_id,
        t.csat AS ticket_csat,
        su.team AS agent_team,
        su.agent_name
    FROM cleaned s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
line_level AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.actor,
        s.ticket_csat,
        s.agent_team,
        s.agent_name,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_at AS event_event_at
    FROM joined_ticket s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
joined_ticket_2 AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.agent_team,
        s.agent_name,
        s.event_id,
        t.requester_id AS ticket_requester_id,
        t.closed_at AS ticket_closed_at,
        t.opened_at AS ticket_opened_at
    FROM line_level s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.requester_id >= 9
),
with_details AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.agent_team,
        s.agent_name,
        s.event_id,
        s.ticket_requester_id,
        s.ticket_closed_at,
        t.opened_at AS ticket_opened_at_2,
        t.priority AS ticket_priority
    FROM joined_ticket_2 s
    JOIN tickets t ON t.id = s.ticket_id
),
ticket_id_stats AS (
    -- Max ticket_requester_id per ticket_id
    SELECT ticket_id, MAX(ticket_requester_id) AS max_ticket_requester_id
    FROM with_details
    GROUP BY ticket_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_ticket_requester_id,
        ROUND(s.ticket_requester_id - g.max_ticket_requester_id, 2) AS ticket_requester_id_minus_max
    FROM with_details s
    JOIN ticket_id_stats g ON g.ticket_id = s.ticket_id
    WHERE s.ticket_requester_id = g.max_ticket_requester_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER w_event_id_ticket_id_by_ticket AS ticket_requester_id_minus_max_dense_rank,
        SUM(ticket_requester_id) OVER (PARTITION BY event_id, ticket_id ORDER BY ticket_requester_id_minus_max, id, event_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ticket_requester_id_sum_7,
        MIN(ticket_requester_id) OVER (PARTITION BY event_id, ticket_id ORDER BY ticket_requester_id_minus_max, id, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_ticket_requester_id_so_far
    FROM above_average
    WINDOW w_event_id_ticket_id_by_ticket AS (PARTITION BY event_id, ticket_id ORDER BY ticket_requester_id_minus_max)
)
SELECT
    id,
    ticket_id,
    agent_name,
    event_id,
    ticket_requester_id,
    ticket_opened_at_2,
    ticket_priority,
    max_ticket_requester_id,
    ticket_requester_id_minus_max,
    ticket_requester_id_minus_max_dense_rank,
    ticket_requester_id_sum_7,
    min_ticket_requester_id_so_far,
    ROUND(max_ticket_requester_id * 1.07, 2) AS max_ticket_requester_id_adj,
    LOWER(REGEXP_REPLACE(TRIM(ticket_priority), ' ', '_')) AS ticket_priority_slug
FROM ranked
ORDER BY ticket_requester_id_minus_max_dense_rank DESC, ticket_requester_id_sum_7 DESC
