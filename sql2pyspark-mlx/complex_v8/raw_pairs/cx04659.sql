WITH base_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM ticket_events
    WHERE event_type = 'replied'
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER w_actor_or_unknown_by_event_at AS event_at_seq,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor_or_unknown ORDER BY event_at ASC NULLS LAST, id), 3) AS event_at_percent_rank,
        LAG(event_at) OVER w_actor_or_unknown_by_event_at AS prev_event_at,
        ROW_NUMBER() OVER (PARTITION BY actor_or_unknown ORDER BY event_at ASC NULLS LAST, id) AS rn,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY actor_or_unknown ORDER BY event_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_2,
        DENSE_RANK() OVER (PARTITION BY actor_or_unknown ORDER BY event_at ASC NULLS LAST) AS event_at_dense_rank
    FROM base_events
    WINDOW w_actor_or_unknown_by_event_at AS (PARTITION BY actor_or_unknown ORDER BY event_at, id)
),
top_rows AS (
    -- Top 5 per group
    SELECT *
    FROM windowed
    WHERE rn <= 5
      AND ticket_id >= 26
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor_or_unknown,
    event_at_percent_rank,
    rn,
    second_event_at,
    second_event_at_2,
    CASE WHEN actor_or_unknown IN ('customer', 'system') THEN 'group_a' WHEN actor_or_unknown = 'UNKNOWN' THEN 'group_b' ELSE 'other' END AS actor_or_unknown_group
FROM top_rows
ORDER BY event_at_percent_rank DESC NULLS FIRST, second_event_at_2, id
LIMIT 10
