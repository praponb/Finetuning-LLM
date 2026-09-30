WITH recent AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE event_type IS NOT NULL
      AND ticket_id >= 59
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        ticket_id,
        days_since_event_at,
        LENGTH(TRIM(actor)) AS actor_len,
        CONCAT(event_type, '#', CAST(id AS STRING)) AS event_type_ref
    FROM recent
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        days_since_event_at,
        event_type_ref,
        RANK() OVER (PARTITION BY actor_len, ticket_id ORDER BY actor_len ASC) AS actor_len_rank,
        days_since_event_at - LAG(days_since_event_at) OVER (PARTITION BY actor_len, ticket_id ORDER BY actor_len ASC, id) AS days_since_event_at_change
    FROM tagged
)
-- Subtotals with ROLLUP
SELECT
    actor_len_rank,
    ticket_id,
    GROUPING(actor_len_rank) AS is_total_actor_len_rank,
    COUNT(*) AS cnt,
    ROUND(STDDEV(days_since_event_at), 2) AS stddev_days_since_event_at
FROM metrics
GROUP BY ROLLUP(actor_len_rank, ticket_id)
ORDER BY actor_len_rank NULLS LAST, ticket_id NULLS LAST
