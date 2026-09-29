WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor = 'agent'
      AND MONTH(event_at) IN (2, 3)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_type,
        ROUND(CUME_DIST() OVER (ORDER BY event_at DESC, id), 3) AS event_at_cume_dist,
        FIRST_VALUE(event_at) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        NTILE(10) OVER (ORDER BY event_at DESC, id) AS event_at_decile,
        LAG(event_at) OVER w_all_by_event_at AS prev_event_at,
        ROW_NUMBER() OVER (ORDER BY event_at DESC, id) AS seq_no
    FROM scoped
    WINDOW w_all_by_event_at AS (ORDER BY event_at DESC, id)
),
derived AS (
    -- Label and score rows
    SELECT *, COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM metrics
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.event_at_cume_dist,
        s.prev_event_at,
        t.id AS event_id,
        t.event_at,
        t.actor AS event_actor
    FROM derived s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at,
        (SELECT COUNT(*) FILTER (WHERE t.actor = 'system') FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_actor_count,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at_2
    FROM with_children_events s
)
SELECT
    id,
    ticket_id,
    event_type,
    event_at_cume_dist,
    prev_event_at,
    event_id,
    event_actor,
    first_event_at,
    event_actor_count,
    TO_DATE(first_event_at_2) AS first_event_at_2_date
FROM with_counts
WHERE first_event_at_2 < TIMESTAMP '2025-02-17 09:00:00'
