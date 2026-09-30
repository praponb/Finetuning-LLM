WITH scoped AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(TRIM(actor)) AS actor_clean
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-25 10:00:00'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        event_at,
        event_type,
        LAST_VALUE(event_at) OVER (PARTITION BY actor, event_type ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        FIRST_VALUE(event_at) OVER (PARTITION BY actor, event_type ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        NTILE(4) OVER w_actor_event_type_by_event_at_2 AS event_at_quartile,
        ROW_NUMBER() OVER (PARTITION BY actor, event_type ORDER BY event_at ASC, id) AS seq_no
    FROM scoped
    WINDOW w_actor_event_type_by_event_at_2 AS (PARTITION BY actor, event_type ORDER BY event_at ASC, id)
)
-- Subtotals with ROLLUP
SELECT
    event_at_quartile,
    COALESCE(event_type, 'ALL') AS event_type,
    ROUND(SUM(event_at_quartile) / NULLIF(SUM(seq_no), 0), 3) AS event_at_quartile_to_seq_no_ratio
FROM with_trends
GROUP BY ROLLUP(event_at_quartile, event_type)
ORDER BY event_at_quartile NULLS LAST, event_type NULLS LAST
