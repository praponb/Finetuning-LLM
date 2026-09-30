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
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type ORDER BY event_at DESC NULLS FIRST), 3) AS event_at_percent_rank,
        FIRST_VALUE(event_at) OVER (PARTITION BY priority ORDER BY hours_since_open, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at
    FROM joined_ranges
),
exploded_detail_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.agent_id,
        s.hours_since_open,
        s.event_at_percent_rank,
        s.first_event_at,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type AS event_event_type,
        t.event_at AS event_event_at
    FROM with_windows s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.agent_id,
        s.hours_since_open,
        s.event_at_percent_rank,
        s.first_event_at,
        s.event_id,
        s.event_actor,
        s.event_event_type,
        s.event_event_at,
        t.ticket_id AS event_ticket_id,
        t.event_at
    FROM exploded_detail_events s
    JOIN ticket_events t ON t.id = s.event_id
    WHERE t.ticket_id <= 30
)
SELECT id, event_id, metric, value
FROM (
    SELECT id, event_id, event_at_percent_rank, hours_since_open
    FROM enriched
) src
UNPIVOT INCLUDE NULLS (
    value FOR metric IN (event_at_percent_rank, hours_since_open)
) unp
