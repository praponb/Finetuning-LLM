WITH filtered AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        HOUR(event_at) AS event_at_hour
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND event_type <> 'resolved'
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN actor IN ('system', 'customer') THEN 'group_a' WHEN actor = 'agent' THEN 'group_b' ELSE 'other' END AS actor_group,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
    FROM filtered
),
enriched_2 AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.event_at_hour,
        s.actor_group,
        t.csat AS ticket_csat,
        t.priority AS ticket_priority
    FROM enriched s
    JOIN tickets t ON t.id = s.ticket_id AND ARRAY_CONTAINS(t.tags, 'login')
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(actor_group, 'ALL') AS actor_group,
    ticket_csat,
    ROUND(STDDEV(ticket_csat), 2) AS stddev_ticket_csat
FROM enriched_2
GROUP BY GROUPING SETS ((actor_group, ticket_csat), (actor_group))
