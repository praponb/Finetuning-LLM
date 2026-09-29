WITH scoped AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type IN ('created', 'assigned', 'escalated')
      AND actor IN ('customer', 'system')
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        t.agent_id AS ticket_agent_id
    FROM scoped s
    JOIN tickets t ON t.id = s.ticket_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        ticket_agent_id,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        FIRST_VALUE(event_at) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY event_at NULLS LAST, id) AS rn,
        ROUND(CUME_DIST() OVER w_ticket_id_by_event_at_ties, 3) AS event_at_cume_dist,
        LAG(event_at) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at, id) AS prev_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_ticket_id_by_event_at_2)) / 60, 1) AS minutes_since_prev_event_at,
        LAG(event_at) OVER w_ticket_id_by_event_at_2 AS prev_event_at_2,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at_2,
        RANK() OVER (PARTITION BY ticket_id, event_type ORDER BY event_at) AS event_at_rank
    FROM with_parent
    WINDOW w_ticket_id_by_event_at_ties AS (PARTITION BY ticket_id ORDER BY event_at ASC), w_ticket_id_by_event_at_2 AS (PARTITION BY ticket_id ORDER BY event_at ASC, id)
),
first_rows AS (
    -- Top 2 per group
    SELECT *
    FROM ranked
    WHERE rn <= 2
)
SELECT
    id,
    ticket_id,
    event_at,
    last_known_event_at,
    second_event_at,
    prev_event_at,
    minutes_since_prev_event_at,
    prev_event_at_2,
    minutes_since_prev_event_at_2,
    event_at_rank,
    DATE_TRUNC('MONTH', event_at) AS event_at_month
FROM first_rows
WHERE minutes_since_prev_event_at < 4000.0
ORDER BY second_event_at, event_at
