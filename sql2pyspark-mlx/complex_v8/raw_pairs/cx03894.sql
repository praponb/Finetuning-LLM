WITH valid_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN actor IN ('customer', 'agent') THEN 'group_a' WHEN actor = 'system' THEN 'group_b' ELSE 'other' END AS actor_group,
        LOWER(TRIM(event_type)) AS event_type_clean
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-03-08 09:00:00'
      AND ticket_id IS NOT NULL
),
with_details_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor_group,
        s.event_type_clean,
        t.closed_at AS ticket_closed_at,
        t.tags AS ticket_tags,
        t.csat AS ticket_csat
    FROM valid_events s
    JOIN tickets t ON t.id = s.ticket_id
),
totals AS (
    -- Summarise per group
    SELECT
        ticket_id,
        COUNT(*) FILTER (WHERE event_type = 'created') AS n_event_type_created,
        COUNT(*) AS row_count
    FROM with_details_ticket
    WHERE ticket_csat < 4
    GROUP BY ticket_id
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.ticket_id,
        s.n_event_type_created,
        s.row_count,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_at
    FROM totals s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id AND TO_DATE(t.event_at) < DATE '2025-02-24'
)
-- Final projection
SELECT
    ticket_id,
    n_event_type_created,
    row_count,
    event_id,
    event_actor,
    event_at
FROM exploded_detail
WHERE TO_DATE(event_at) < DATE '2025-03-14'
ORDER BY row_count, event_id DESC, ticket_id
LIMIT 20
