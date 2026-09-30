WITH prepared AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown,
        UPPER(TRIM(actor)) AS actor_clean
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-31 08:00:00'
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN event_type IN ('replied', 'resolved') THEN 'group_a' WHEN event_type = 'escalated' THEN 'group_b' ELSE 'other' END AS event_type_group,
        CASE WHEN event_type IN ('created', 'resolved') THEN 'group_a' WHEN event_type = 'reopened' THEN 'group_b' ELSE 'other' END AS event_type_group_2
    FROM prepared
    WHERE event_type NOT IN ('created', 'resolved')
)
-- Subtotals with GROUPING SETS
SELECT
    actor,
    actor_or_unknown,
    ticket_id,
    COUNT(*) FILTER (WHERE actor <> 'customer') AS n_actor_customer
FROM tagged
GROUP BY GROUPING SETS ((actor, actor_or_unknown, ticket_id), (actor), (actor_or_unknown), ())
ORDER BY actor NULLS LAST, actor_or_unknown NULLS LAST, ticket_id NULLS LAST
