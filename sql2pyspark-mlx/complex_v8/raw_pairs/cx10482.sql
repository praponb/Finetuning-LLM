WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug
    FROM ticket_events
),
with_details_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.actor_slug,
        t.closed_at AS ticket_closed_at,
        t.agent_id AS ticket_agent_id,
        t.requester_id AS ticket_requester_id,
        su.hired_on AS agent_hired_on
    FROM base s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        event_at,
        actor,
        ticket_closed_at,
        ticket_agent_id,
        ticket_requester_id,
        agent_hired_on,
        ROUND(ticket_requester_id / NULLIF(SUM(ticket_requester_id) OVER w_ticket_id_ticket_agent_id, 0), 4) AS ticket_requester_id_share_of_ticket_id_ticket_ag,
        LAG(agent_hired_on, 2) OVER (PARTITION BY event_type ORDER BY agent_hired_on, id) AS prev2_agent_hired_on,
        SIZE(COLLECT_SET(event_type) OVER (PARTITION BY ticket_id, ticket_agent_id)) AS distinct_event_type_in_ticket_id_ticket_agent_id,
        ROUND(ticket_requester_id / NULLIF(SUM(ticket_requester_id) OVER w_actor, 0), 4) AS ticket_requester_id_share_of_actor,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(agent_hired_on) ORDER BY ticket_closed_at), 3) AS ticket_closed_at_cume_dist,
        ROUND(ticket_requester_id - AVG(ticket_requester_id) OVER (PARTITION BY actor), 2) AS ticket_requester_id_vs_actor_avg,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY MONTH(agent_hired_on) ORDER BY ticket_closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        ROUND(CUME_DIST() OVER w_agent_hired_on_month_by_tick_3, 3) AS ticket_closed_at_cume_dist_2,
        MAX(ticket_requester_id) OVER (PARTITION BY ticket_id ORDER BY ticket_closed_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ticket_requester_id_max_7,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_agent_id, event_type ORDER BY ticket_requester_id ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at
    FROM with_details_ticket
    WINDOW w_ticket_id_ticket_agent_id AS (PARTITION BY ticket_id, ticket_agent_id), w_actor AS (PARTITION BY actor), w_agent_hired_on_month_by_tick_3 AS (PARTITION BY MONTH(agent_hired_on) ORDER BY ticket_closed_at, id)
),
enriched AS (
    -- Derive helper columns
    SELECT *, TO_DATE(second_event_at) AS second_event_at_date
    FROM metrics
)
-- Final projection
SELECT
    id,
    ticket_closed_at,
    agent_hired_on,
    ticket_requester_id_share_of_ticket_id_ticket_ag,
    prev2_agent_hired_on,
    distinct_event_type_in_ticket_id_ticket_agent_id,
    ticket_requester_id_share_of_actor,
    ticket_closed_at_cume_dist,
    ticket_requester_id_vs_actor_avg,
    first_known_event_at,
    ticket_closed_at_cume_dist_2,
    second_event_at,
    second_event_at_date,
    TO_DATE(event_at) AS event_at_date,
    COALESCE(ticket_requester_id, 0) AS ticket_requester_id_filled
FROM enriched
