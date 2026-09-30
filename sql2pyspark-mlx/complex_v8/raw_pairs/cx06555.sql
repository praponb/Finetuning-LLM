WITH recent AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN event_type IN ('assigned', 'reopened') THEN 'group_a' WHEN event_type = 'created' THEN 'group_b' ELSE 'other' END AS event_type_group,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at,
        SUBSTRING(TRIM(actor), 1, 4) AS actor_prefix
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
line_level AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.event_type_group,
        s.actor_prefix,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.event_at AS event_event_at,
        t.actor AS event_actor
    FROM recent s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
latest AS (
    -- Keep the latest row per ticket_id, event_event_type
    SELECT
        id,
        ticket_id,
        event_type,
        event_type_group,
        actor_prefix,
        event_id,
        event_event_type,
        event_event_at,
        event_actor
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id, event_event_type ORDER BY event_event_at DESC, id DESC, event_id DESC) AS recency_rank
        FROM line_level
    ) ranked
    WHERE recency_rank = 1
)
-- Subtotals with GROUPING SETS
SELECT
    event_event_type,
    event_type,
    GROUPING(event_event_type) AS is_total_event_event_type,
    COUNT(*) AS cnt,
    COUNT(DISTINCT event_event_type) AS distinct_event_event_types
FROM latest
GROUP BY GROUPING SETS ((event_event_type, event_type), (event_event_type))
