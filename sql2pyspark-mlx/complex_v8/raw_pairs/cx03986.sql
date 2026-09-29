WITH cleaned_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(actor, '#', CAST(id AS STRING)) AS actor_ref,
        CONCAT(actor, '#', CAST(ticket_id AS STRING)) AS actor_ref_2
    FROM ticket_events
    WHERE event_type IN ('resolved', 'escalated')
      AND actor = 'customer'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_at,
        event_type,
        actor,
        actor_ref,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY event_type, actor ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROW_NUMBER() OVER w_event_type_actor_by_event_at_2 AS event_at_seq,
        ROUND(CUME_DIST() OVER (PARTITION BY actor ORDER BY event_at, id), 3) AS event_at_cume_dist,
        DENSE_RANK() OVER (PARTITION BY actor ORDER BY event_at, id) AS event_at_dense_rank
    FROM cleaned_events
    WINDOW w_event_type_actor_by_event_at_2 AS (PARTITION BY event_type, actor ORDER BY event_at DESC, id)
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(second_event_at) - UNIX_TIMESTAMP(event_at)) / 3600, 1) AS hours_event_at_to_second_event_at,
        ROUND(event_at_dense_rank / NULLIF(event_at_cume_dist, 0), 3) AS event_at_dense_rank_per_event_at_cume_dist,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
    FROM sequenced
)
SELECT
    id,
    event_at,
    actor,
    actor_ref,
    event_at_seq,
    event_at_cume_dist,
    event_at_dense_rank,
    hours_event_at_to_second_event_at,
    event_at_dense_rank_per_event_at_cume_dist,
    event_type_slug
FROM enriched
ORDER BY event_at_dense_rank_per_event_at_cume_dist DESC
