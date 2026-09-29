WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(TRIM(actor)) AS actor_clean,
        INITCAP(TRIM(actor)) AS actor_clean_2,
        LENGTH(TRIM(event_type)) AS event_type_len
    FROM ticket_events
    WHERE event_type <> 'assigned'
      AND actor = 'agent'
),
joined AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.actor_clean,
        s.actor_clean_2,
        s.event_type_len,
        t.closed_at AS ticket_closed_at
    FROM valid s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY TO_DATE(event_at), MONTH(ticket_closed_at) ORDER BY event_type_len, id) AS event_type_len_seq,
        SIZE(COLLECT_SET(event_type) OVER (PARTITION BY ticket_id, actor)) AS distinct_event_type_in_ticket_id_actor,
        MAX(event_type_len) OVER w_ticket_id_actor AS ticket_id_actor_max_event_type_len,
        DENSE_RANK() OVER w_event_at_to_date_ticket_clos_2 AS event_type_len_dense_rank
    FROM joined
    WINDOW w_ticket_id_actor AS (PARTITION BY ticket_id, actor), w_event_at_to_date_ticket_clos_2 AS (PARTITION BY TO_DATE(event_at), MONTH(ticket_closed_at) ORDER BY event_type_len)
),
detail_rows_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor_clean_2,
        s.ticket_closed_at,
        s.distinct_event_type_in_ticket_id_actor,
        s.ticket_id_actor_max_event_type_len,
        s.event_type_len_dense_rank,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.actor AS event_actor
    FROM with_trends s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at_2
    FROM detail_rows_events s
),
with_counts_2 AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at_3
    FROM with_counts s
)
-- Subtotals with CUBE
SELECT
    COALESCE(event_type, 'ALL') AS event_type,
    COALESCE(event_actor, 'ALL') AS event_actor,
    MAX(last_event_at_2) AS last_last_event_at_2
FROM with_counts_2
GROUP BY CUBE(event_type, event_actor)
