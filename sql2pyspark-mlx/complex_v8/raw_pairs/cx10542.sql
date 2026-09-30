WITH valid_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(actor, '#', CAST(ticket_id AS STRING)) AS actor_ref,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label
    FROM ticket_events
    WHERE ticket_id <= 10
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        event_at,
        event_type,
        actor,
        event_at_label,
        LEAD(event_at) OVER (PARTITION BY event_type, actor ORDER BY event_at DESC, id) AS next_event_at,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type, actor ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        LEAD(event_at) OVER (ORDER BY event_at DESC NULLS FIRST, id) AS next_event_at_2,
        DENSE_RANK() OVER (PARTITION BY event_type, actor ORDER BY event_at DESC) AS event_at_dense_rank,
        NTILE(3) OVER (ORDER BY event_at DESC NULLS FIRST, id) AS event_at_tercile
    FROM valid_events
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_trends
    WHERE event_at_dense_rank > (SELECT AVG(event_at_dense_rank) FROM with_trends)
)
-- Final projection
SELECT
    id,
    event_at,
    event_type,
    actor,
    event_at_label,
    next_event_at_2
FROM without_matches
ORDER BY event_at DESC
