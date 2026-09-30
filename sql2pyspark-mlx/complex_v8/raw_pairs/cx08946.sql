WITH cleaned_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN actor IN ('agent', 'customer') THEN 'group_a' WHEN actor = 'system' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM ticket_events
    WHERE event_type = 'escalated'
),
with_details AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        t.requester_id AS ticket_requester_id,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier
    FROM cleaned_events s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
    WHERE t.requester_id >= 11
),
joined_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.ticket_requester_id,
        s.agent_hired_on,
        t.csat AS ticket_csat,
        su.agent_name
    FROM with_details s
    JOIN tickets t ON t.id = s.ticket_id AND HOUR(t.closed_at) BETWEEN 6 AND 10
    LEFT JOIN support_agents su ON su.id = t.agent_id
    WHERE t.csat > 3
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY ticket_csat, ticket_id) AS rows_in_ticket_csat_ticket_id,
        LAST_VALUE(agent_hired_on) OVER (PARTITION BY ticket_csat, ticket_id ORDER BY agent_hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_agent_hired_on,
        RANK() OVER w_ticket_csat_ticket_id_by_age_2 AS agent_hired_on_rank
    FROM joined_ticket
    WINDOW w_ticket_csat_ticket_id_by_age_2 AS (PARTITION BY ticket_csat, ticket_id ORDER BY agent_hired_on, id)
)
SELECT
    id,
    ticket_id,
    ticket_requester_id,
    agent_hired_on,
    ticket_csat,
    agent_name,
    last_agent_hired_on,
    DATE_TRUNC('MONTH', event_at) AS event_at_month
FROM with_trends
ORDER BY ticket_id DESC
