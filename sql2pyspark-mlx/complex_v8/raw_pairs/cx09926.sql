WITH joined_ranges AS (
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
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.priority,
        s.agent_id,
        s.hours_since_open,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type AS event_event_type,
        t.event_at AS event_event_at
    FROM joined_ranges s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id
)
SELECT
    ticket_id,
    agent_id,
    GROUPING_ID() AS gid,
    MIN(event_event_at) AS first_event_event_at,
    MAX(event_event_at) AS last_event_event_at,
    SUM(CASE WHEN event_type IN ('replied', 'created') THEN 1 ELSE 0 END) AS n_event_type_in_replied_create
FROM with_children_events
GROUP BY GROUPING SETS ((ticket_id, agent_id), (ticket_id), (agent_id))
ORDER BY ticket_id NULLS LAST, agent_id NULLS LAST
