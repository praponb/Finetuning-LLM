WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(actor, '#', CAST(ticket_id AS STRING)) AS actor_ref
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_id, actor ORDER BY event_at DESC), 3) AS event_at_percent_rank,
        LAG(event_at, 2) OVER w_ticket_id_actor_by_event_at_2 AS prev2_event_at
    FROM cleaned
    WINDOW w_ticket_id_actor_by_event_at_2 AS (PARTITION BY ticket_id, actor ORDER BY event_at DESC, id)
),
detail_rows_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_at_percent_rank,
        s.prev2_event_at,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.event_type AS event_event_type
    FROM sequenced s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        t.ticket_id AS event_ticket_id,
        t.event_type AS event_event_type_2,
        t.actor AS event_actor,
        ti.csat AS ticket_csat
    FROM detail_rows_events s
    LEFT JOIN ticket_events t ON t.id = s.event_id
    LEFT JOIN tickets ti ON ti.id = s.ticket_id
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(actor, 'ALL') AS actor,
    COALESCE(event_type, 'ALL') AS event_type,
    SUM(event_at_percent_rank) AS total_event_at_percent_rank
FROM combined_details
GROUP BY GROUPING SETS ((actor, event_type), (actor), (event_type), ())
ORDER BY actor NULLS LAST, event_type NULLS LAST
