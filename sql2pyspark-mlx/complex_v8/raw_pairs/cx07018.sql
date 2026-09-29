WITH filtered AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
        LOWER(TRIM(event_type)) AS event_type_clean
    FROM ticket_events
    WHERE (MONTH(event_at) IN (1, 2) OR event_type IS NOT NULL)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY event_at DESC NULLS FIRST) AS event_at_dense_rank,
        FIRST_VALUE(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        RANK() OVER (PARTITION BY ticket_id, event_type ORDER BY event_at DESC) AS event_at_rank
    FROM filtered
),
with_details_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.actor,
        s.actor_slug,
        s.event_at_dense_rank,
        s.first_event_at,
        s.event_at_rank,
        t.tags AS ticket_tags,
        t.agent_id AS ticket_agent_id
    FROM windowed s
    JOIN tickets t ON t.id = s.ticket_id
)
SELECT id, measure, metric_value
FROM (
    SELECT id, event_at_rank, event_at_dense_rank
    FROM with_details_ticket
) src
UNPIVOT (
    metric_value FOR measure IN (event_at_rank, event_at_dense_rank)
) unp
