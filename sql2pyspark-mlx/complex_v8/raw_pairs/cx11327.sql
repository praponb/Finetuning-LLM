WITH filtered AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE (event_type IN ('replied', 'escalated', 'resolved') OR event_at < TIMESTAMP '2025-03-08 09:00:00')
),
with_details_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.requester_id AS ticket_requester_id,
        t.tags AS ticket_tags,
        t.opened_at AS ticket_opened_at,
        su.hired_on AS agent_hired_on
    FROM filtered s
    JOIN tickets t ON t.id = s.ticket_id AND t.priority = 'P2'
    JOIN support_agents su ON su.id = t.agent_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        ticket_requester_id,
        ticket_tags,
        ticket_opened_at,
        SIZE(COLLECT_SET(event_type) OVER (PARTITION BY ticket_id, actor)) AS distinct_event_type_in_ticket_id_actor,
        MIN(ticket_requester_id) OVER (PARTITION BY ticket_id, actor) AS ticket_id_actor_min_ticket_requester_id,
        NTILE(10) OVER (PARTITION BY event_type, MONTH(event_at) ORDER BY ticket_requester_id DESC, id) AS ticket_requester_id_decile,
        ROW_NUMBER() OVER (PARTITION BY actor ORDER BY ticket_opened_at DESC, id) AS rn,
        ROUND(ticket_requester_id - AVG(ticket_requester_id) OVER w_ticket_id, 2) AS ticket_requester_id_vs_ticket_id_avg,
        SUM(ticket_requester_id) OVER (PARTITION BY actor, YEAR(agent_hired_on) ORDER BY ticket_opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ticket_requester_id,
        MIN(ticket_requester_id) OVER w_ticket_id AS ticket_id_min_ticket_requester_id,
        ROUND(AVG(ticket_requester_id) OVER (PARTITION BY ticket_id, actor), 2) AS ticket_id_actor_avg_ticket_requester_id,
        MAX(ticket_requester_id) OVER (PARTITION BY actor ORDER BY ticket_opened_at DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS ticket_requester_id_max_3_prior
    FROM with_details_ticket
    WINDOW w_ticket_id AS (PARTITION BY ticket_id)
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    ticket_requester_id,
    ticket_tags,
    ticket_opened_at,
    distinct_event_type_in_ticket_id_actor,
    ticket_id_actor_min_ticket_requester_id,
    ticket_requester_id_decile,
    rn,
    ticket_requester_id_vs_ticket_id_avg,
    running_ticket_requester_id,
    ticket_id_min_ticket_requester_id,
    ticket_id_actor_avg_ticket_requester_id,
    ticket_requester_id_max_3_prior,
    FLOOR(ticket_requester_id / 5) * 5 AS ticket_requester_id_bucket
FROM sequenced
WHERE rn BETWEEN 3 AND 7
ORDER BY distinct_event_type_in_ticket_id_actor DESC NULLS FIRST
