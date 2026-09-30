WITH matched AS (
    -- Events that happened while the ticket was open
    SELECT
        e.id,
        e.ticket_id,
        e.event_at,
        e.event_type,
        t.priority,
        t.agent_id,
        ROUND((UNIX_TIMESTAMP(e.event_at) - UNIX_TIMESTAMP(t.opened_at)) / 3600, 2) AS hours_since_open
    FROM ticket_events e
    JOIN tickets t
      ON t.id = e.ticket_id
     AND e.event_at >= t.opened_at
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 7 DAYS)
),
enriched AS (
    -- Join support_agents
    SELECT
        s.id,
        s.ticket_id,
        s.priority,
        s.agent_id,
        su.team AS agent_team,
        su.agent_name,
        t.opened_at AS ticket_opened_at
    FROM matched s
    JOIN support_agents su ON su.id = s.agent_id
    LEFT JOIN tickets t ON t.id = s.ticket_id
    WHERE UPPER(TRIM(su.agent_name)) LIKE 'F%'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY agent_id ORDER BY ticket_opened_at DESC), 3) AS ticket_opened_at_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY agent_id ORDER BY ticket_opened_at DESC), 3) AS ticket_opened_at_percent_rank,
        FIRST_VALUE(ticket_opened_at) OVER (PARTITION BY agent_id ORDER BY ticket_opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ticket_opened_at,
        NTILE(5) OVER (PARTITION BY agent_id ORDER BY ticket_opened_at DESC, id) AS ticket_opened_at_quintile
    FROM enriched
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, reading
FROM (
    SELECT id, ticket_opened_at_percent_rank, ticket_opened_at_cume_dist
    FROM sequenced
) src
UNPIVOT (
    reading FOR field IN (ticket_opened_at_percent_rank, ticket_opened_at_cume_dist)
) unp
ORDER BY id, field
