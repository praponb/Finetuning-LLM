WITH recent AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('WEEK', event_at) AS event_at_week,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
deduped AS (
    -- Keep the latest row per event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_week,
        event_at_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_at DESC, id DESC) AS recency_rank
        FROM recent
    ) ranked
    WHERE recency_rank = 1
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LAG(event_at_week) OVER w_actor_ticket_id_by_event_at AS prev_event_at_week,
        ROUND(PERCENT_RANK() OVER w_actor_ticket_id_by_event_at, 3) AS event_at_week_percent_rank,
        DENSE_RANK() OVER (PARTITION BY actor, ticket_id ORDER BY event_at_week DESC, id) AS event_at_week_dense_rank,
        RANK() OVER (PARTITION BY actor, ticket_id ORDER BY event_at_week DESC, id) AS event_at_week_rank,
        RANK() OVER (PARTITION BY actor, ticket_id ORDER BY event_at_week DESC) AS event_at_week_rank_3
    FROM deduped
    WINDOW w_actor_ticket_id_by_event_at AS (PARTITION BY actor, ticket_id ORDER BY event_at_week DESC, id)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    event_at_week_rank,
    ROUND((UNIX_TIMESTAMP(prev_event_at_week) - UNIX_TIMESTAMP(event_at)) / 3600, 1) AS hours_event_at_to_prev_event_at_week,
    DATE_FORMAT(prev_event_at_week, 'yyyy-MM') AS prev_event_at_week_label
FROM sequenced
ORDER BY event_type DESC, event_at_week_rank, id
LIMIT 10
