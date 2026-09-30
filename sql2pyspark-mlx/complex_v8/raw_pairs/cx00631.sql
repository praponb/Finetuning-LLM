WITH prepared_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(TRIM(actor)) AS actor_clean,
        CONCAT(event_type, '#', CAST(id AS STRING)) AS event_type_ref
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY event_at DESC) AS event_at_dense_rank,
        RANK() OVER w_all_by_event_at AS event_at_rank,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at
    FROM prepared_events
    WINDOW w_all_by_event_at AS (ORDER BY event_at DESC, id)
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(actor, 'ALL') AS actor,
    event_at_dense_rank,
    GROUPING_ID() AS grouping_level,
    COUNT(DISTINCT actor) AS distinct_actors,
    ROUND(AVG(event_at_rank), 2) AS avg_event_at_rank,
    SUM(event_at_rank) AS total_event_at_rank
FROM sequenced
GROUP BY ROLLUP(actor, event_at_dense_rank)
ORDER BY actor NULLS LAST, event_at_dense_rank NULLS LAST
