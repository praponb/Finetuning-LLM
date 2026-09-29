WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
        CASE WHEN event_type IN ('resolved', 'escalated') THEN 'group_a' WHEN event_type = 'created' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM ticket_events
    WHERE ticket_id <= 16
      AND HOUR(event_at) BETWEEN 10 AND 16
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        actor_slug,
        event_type_group,
        NTILE(4) OVER (ORDER BY event_at ASC, id) AS event_at_quartile,
        DENSE_RANK() OVER (ORDER BY event_at DESC) AS event_at_dense_rank,
        DENSE_RANK() OVER (PARTITION BY event_type, actor ORDER BY event_at DESC, id) AS event_at_dense_rank_2,
        ROUND(CUME_DIST() OVER w_ticket_id_event_type_by_even, 3) AS event_at_cume_dist,
        LAST_VALUE(event_at) OVER (ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        ROW_NUMBER() OVER (PARTITION BY event_type, actor ORDER BY event_at DESC, id) AS rn
    FROM scoped
    WINDOW w_ticket_id_event_type_by_even AS (PARTITION BY ticket_id, event_type ORDER BY event_at), w_event_type_actor_by_event_at AS (PARTITION BY event_type, actor ORDER BY event_at DESC, id)
),
top_per_group AS (
    -- Top 5 per group
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        actor_slug,
        event_type_group,
        event_at_quartile,
        event_at_dense_rank_2,
        event_at_cume_dist,
        last_event_at,
        rn
    FROM ranked
    WHERE event_at_dense_rank <= 5
)
SELECT
    id,
    ticket_id,
    event_type,
    actor,
    actor_slug,
    event_at_quartile,
    event_at_dense_rank_2,
    event_at_cume_dist,
    last_event_at,
    rn
FROM top_per_group
ORDER BY actor_slug, event_at_dense_rank_2
