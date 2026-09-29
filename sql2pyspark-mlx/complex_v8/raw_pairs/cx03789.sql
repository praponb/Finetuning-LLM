WITH prepared AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-03-08 08:00:00'
),
with_details AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        t.agent_id AS ticket_agent_id
    FROM prepared s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        ticket_agent_id,
        LENGTH(TRIM(actor)) AS actor_len,
        LENGTH(TRIM(event_type)) AS event_type_len
    FROM with_details
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        actor,
        ticket_agent_id,
        actor_len,
        event_type_len,
        ROUND(event_type_len - AVG(event_type_len) OVER w_ticket_agent_id, 2) AS event_type_len_vs_ticket_agent_id_avg,
        ROUND(AVG(actor_len) OVER (PARTITION BY event_type, event_type_len ORDER BY event_type_len, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS actor_len_avg_7,
        ROW_NUMBER() OVER (PARTITION BY event_type_len, actor_len ORDER BY actor_len DESC, id) AS rn,
        FIRST_VALUE(event_type_len) IGNORE NULLS OVER (PARTITION BY event_type, event_type_len ORDER BY event_type_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_type_len,
        LAST_VALUE(actor_len) OVER (PARTITION BY event_type, event_type_len ORDER BY event_type_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_actor_len,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type_len, actor_len ORDER BY actor_len DESC), 3) AS actor_len_cume_dist,
        RANK() OVER (PARTITION BY event_type_len, actor_len ORDER BY actor_len DESC) AS actor_len_rank
    FROM tagged
    WINDOW w_ticket_agent_id AS (PARTITION BY ticket_agent_id)
)
SELECT
    id,
    ticket_id,
    actor,
    ticket_agent_id,
    event_type_len,
    event_type_len_vs_ticket_agent_id_avg,
    rn,
    first_known_event_type_len,
    last_actor_len,
    actor_len_cume_dist,
    actor_len_rank,
    ROUND(actor_len_cume_dist * 1.07, 2) AS actor_len_cume_dist_adj
FROM ranked
ORDER BY event_type_len DESC
