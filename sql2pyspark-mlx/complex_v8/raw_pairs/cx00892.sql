WITH cleaned_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('WEEK', event_at) AS event_at_week
    FROM ticket_events
    WHERE (MONTH(event_at) IN (2, 3) OR actor NOT IN ('system', 'agent'))
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_at_week,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.actor AS event_actor,
        t.event_at AS event_event_at
    FROM cleaned_events s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.event_type <> 'resolved'
),
deduped AS (
    -- Keep the latest row per event_event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_week,
        event_id,
        event_event_type,
        event_actor,
        event_event_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_event_type ORDER BY event_event_at DESC, id DESC, event_id DESC) AS recency_rank
        FROM with_children_events
    ) ranked
    WHERE recency_rank = 1
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY event_at DESC), 3) AS event_at_cume_dist,
        DENSE_RANK() OVER (ORDER BY event_at DESC) AS event_at_dense_rank,
        FIRST_VALUE(event_at) OVER (ORDER BY event_at DESC, id, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        ROW_NUMBER() OVER (ORDER BY event_at DESC, id, event_id) AS event_at_seq,
        LEAD(event_at_week, 3) OVER w_all_by_event_at AS next3_event_at_week,
        NTILE(10) OVER (ORDER BY event_at DESC, id, event_id) AS event_at_decile
    FROM deduped
    WINDOW w_all_by_event_at AS (ORDER BY event_at DESC, id, event_id)
)
SELECT
    id,
    event_at,
    event_type,
    actor,
    event_at_week,
    event_id,
    event_event_type,
    event_actor,
    event_at_dense_rank,
    first_event_at,
    event_at_seq,
    next3_event_at_week,
    event_at_decile,
    INITCAP(TRIM(event_event_type)) AS event_event_type_clean
FROM metrics
ORDER BY event_event_type_clean DESC
