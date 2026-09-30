WITH filtered AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor IS NOT NULL
      AND event_type = 'replied'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_id, event_type ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at DESC, id))) / 60, 1) AS minutes_since_prev_event_at,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at_2,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROUND(CUME_DIST() OVER w_all_by_event_at, 3) AS event_at_cume_dist,
        DENSE_RANK() OVER (PARTITION BY ticket_id, event_type ORDER BY event_at DESC, id) AS event_at_dense_rank,
        DENSE_RANK() OVER w_event_type_by_event_at_ties AS event_at_dense_rank_2,
        RANK() OVER w_all_by_event_at AS event_at_rank,
        ROW_NUMBER() OVER (ORDER BY event_at ASC, id) AS seq_no,
        LAG(event_at, 3) OVER (PARTITION BY event_type ORDER BY event_at ASC, id) AS prev3_event_at
    FROM filtered
    WINDOW w_all_by_event_at AS (ORDER BY event_at ASC, id), w_event_type_by_event_at_ties AS (PARTITION BY event_type ORDER BY event_at ASC)
)
-- Final projection
SELECT
    id,
    event_at,
    event_type,
    last_known_event_at,
    minutes_since_prev_event_at,
    second_event_at,
    event_at_cume_dist,
    event_at_dense_rank,
    event_at_dense_rank_2,
    event_at_rank,
    CASE WHEN event_type IN ('created', 'resolved') THEN 'group_a' WHEN event_type = 'replied' THEN 'group_b' ELSE 'other' END AS event_type_group,
    CONCAT_WS(' / ', actor, event_type) AS actor_event_type_label
FROM with_windows
ORDER BY event_type_group DESC, event_type
