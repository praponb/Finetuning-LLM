WITH prepared_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor IN ('system', 'agent')
),
enriched_ticket AS (
    -- Join tickets
    SELECT
        s.*,
        t.agent_id AS ticket_agent_id,
        t.csat AS ticket_csat,
        su.hired_on AS agent_hired_on,
        su.agent_name
    FROM prepared_events s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        ticket_agent_id,
        ticket_csat,
        agent_hired_on,
        agent_name,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY event_type, ticket_id ORDER BY agent_hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        MAX(ticket_csat) OVER (PARTITION BY ticket_id ORDER BY ticket_csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_ticket_csat_so_far,
        ROUND(CUME_DIST() OVER w_ticket_id_by_ticket_csat, 3) AS ticket_csat_cume_dist,
        ROUND(AVG(ticket_csat) OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS ticket_csat_avg_2,
        COUNT(*) OVER (PARTITION BY ticket_id, event_type) AS rows_in_ticket_id_event_type,
        ROUND(AVG(ticket_csat) OVER (PARTITION BY ticket_id, event_type), 2) AS ticket_id_event_type_avg_ticket_csat,
        ROUND(CUME_DIST() OVER w_event_type_by_agent_hired_on, 3) AS agent_hired_on_cume_dist,
        ROUND(AVG(ticket_csat) OVER (PARTITION BY event_type ORDER BY agent_hired_on, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS ticket_csat_avg_2_prior,
        FIRST_VALUE(agent_hired_on) OVER (ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_agent_hired_on,
        LAST_VALUE(agent_hired_on) IGNORE NULLS OVER (ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_agent_hired_on,
        MAX(ticket_csat) OVER (PARTITION BY ticket_id, event_type) AS ticket_id_event_type_max_ticket_csat
    FROM enriched_ticket
    WINDOW w_ticket_id_by_ticket_csat AS (PARTITION BY ticket_id ORDER BY ticket_csat, id), w_event_type_by_agent_hired_on AS (PARTITION BY event_type ORDER BY agent_hired_on, id)
)
-- Final projection
SELECT
    id,
    ticket_id,
    agent_name,
    max_ticket_csat_so_far,
    ticket_csat_cume_dist,
    ticket_csat_avg_2,
    rows_in_ticket_id_event_type,
    ticket_id_event_type_avg_ticket_csat,
    agent_hired_on_cume_dist,
    ticket_csat_avg_2_prior,
    first_agent_hired_on
FROM with_trends
WHERE TO_DATE(event_at) < DATE '2025-03-02'
ORDER BY agent_hired_on_cume_dist
