WITH filtered_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-13 11:00:00'
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor_or_unknown,
        s.event_at_label,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.event_type AS event_event_type,
        t.actor AS event_actor
    FROM filtered_events s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(event_event_at) IGNORE NULLS OVER (PARTITION BY event_actor ORDER BY event_at, id, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_event_at,
        NTILE(10) OVER w_event_actor_by_event_event_a AS event_event_at_decile
    FROM exploded_detail
    WINDOW w_event_actor_by_event_event_a AS (PARTITION BY event_actor ORDER BY event_event_at DESC, id, event_id)
)
SELECT
    COALESCE(event_actor, 'ALL') AS event_actor,
    event_event_at_decile,
    GROUPING_ID() AS gid,
    COUNT(DISTINCT event_id) AS distinct_event_ids,
    MAX(event_event_at_decile) AS max_event_event_at_decile
FROM with_trends
GROUP BY GROUPING SETS ((event_actor, event_event_at_decile), (event_actor), (event_event_at_decile), ())
ORDER BY event_actor NULLS LAST, event_event_at_decile NULLS LAST
