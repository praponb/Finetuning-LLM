WITH cleaned AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-03-14'
      AND actor IN ('system', 'customer')
),
joined_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.*,
        t.requester_id AS ticket_requester_id,
        t.priority AS ticket_priority,
        t.opened_at AS ticket_opened_at,
        su.hired_on AS agent_hired_on
    FROM cleaned s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY event_type, MONTH(agent_hired_on) ORDER BY ticket_opened_at ASC, id) AS ticket_opened_at_dense_rank,
        LEAD(ticket_opened_at) OVER w_actor_event_type_by_ticket_o AS next_ticket_opened_at,
        SUM(ticket_requester_id) OVER (PARTITION BY actor, ticket_priority) AS actor_ticket_priority_sum_ticket_requester_id,
        ROUND(ticket_requester_id - AVG(ticket_requester_id) OVER w_event_type_event_at_month, 2) AS ticket_requester_id_vs_event_type_event_at_month,
        ROUND(ticket_requester_id / NULLIF(SUM(ticket_requester_id) OVER (PARTITION BY event_type, MONTH(event_at)), 0), 4) AS ticket_requester_id_share_of_event_type_event_at,
        NTH_VALUE(ticket_requester_id, 2) OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_requester_id,
        RANK() OVER (ORDER BY event_at) AS event_at_rank,
        SUM(ticket_requester_id) OVER (PARTITION BY event_type, MONTH(agent_hired_on) ORDER BY ticket_opened_at ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS ticket_requester_id_sum_2_prior,
        LAG(ticket_requester_id, 1, 0) OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS prev_ticket_requester_id,
        LAG(agent_hired_on) OVER (PARTITION BY actor, event_type ORDER BY ticket_opened_at DESC NULLS FIRST, id) AS prev_agent_hired_on,
        RANK() OVER (PARTITION BY YEAR(event_at) ORDER BY agent_hired_on) AS agent_hired_on_rank,
        SUM(ticket_requester_id) OVER (ORDER BY event_at, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS ticket_requester_id_sum_3_prior
    FROM joined_ticket
    WINDOW w_actor_event_type_by_ticket_o AS (PARTITION BY actor, event_type ORDER BY ticket_opened_at DESC NULLS FIRST, id), w_event_type_event_at_month AS (PARTITION BY event_type, MONTH(event_at))
)
SELECT
    id,
    ticket_id,
    event_at,
    actor,
    ticket_requester_id,
    ticket_opened_at,
    agent_hired_on,
    next_ticket_opened_at,
    actor_ticket_priority_sum_ticket_requester_id,
    ticket_requester_id_vs_event_type_event_at_month,
    ticket_requester_id_share_of_event_type_event_at,
    second_ticket_requester_id,
    event_at_rank,
    ticket_requester_id_sum_2_prior,
    prev_ticket_requester_id,
    prev_agent_hired_on,
    ticket_requester_id_sum_3_prior,
    FLOOR(prev_ticket_requester_id / 5) * 5 AS prev_ticket_requester_id_bucket,
    QUARTER(prev_agent_hired_on) AS prev_agent_hired_on_quarter
FROM ranked
ORDER BY actor, prev_ticket_requester_id DESC
