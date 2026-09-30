WITH recent_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(event_type, '#', CAST(id AS STRING)) AS event_type_ref
    FROM ticket_events
    WHERE ticket_id >= 5
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_type_ref,
        t.tags AS ticket_tags
    FROM recent_events s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE ARRAY_CONTAINS(t.tags, 'bug')
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(event_at) OVER (ORDER BY event_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        FIRST_VALUE(event_at) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at_2,
        NTILE(4) OVER (PARTITION BY event_type ORDER BY event_at ASC, id) AS event_at_quartile,
        LAG(event_at, 2) OVER w_event_type_by_event_at_2 AS prev2_event_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY event_at DESC NULLS FIRST), 3) AS event_at_percent_rank,
        NTILE(5) OVER (ORDER BY event_at DESC, id) AS event_at_quintile,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_2,
        RANK() OVER (PARTITION BY event_type ORDER BY event_at ASC) AS event_at_rank,
        RANK() OVER (ORDER BY event_at DESC NULLS FIRST, id) AS event_at_rank_2,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at
    FROM with_parent
    WINDOW w_event_type_by_event_at_2 AS (PARTITION BY event_type ORDER BY event_at DESC NULLS FIRST, id)
)
SELECT
    id,
    ticket_id,
    event_type,
    actor,
    event_type_ref,
    ticket_tags,
    first_event_at,
    second_event_at,
    first_event_at_2,
    event_at_quartile,
    prev2_event_at,
    event_at_quintile,
    second_event_at_2,
    event_at_rank_2,
    first_known_event_at,
    GREATEST(event_at_percent_rank, event_at_rank) AS greatest_event_at_percent_rank_event_at_rank
FROM with_trends
ORDER BY first_known_event_at DESC NULLS FIRST, event_type_ref
