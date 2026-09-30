WITH prepared_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND actor = 'system'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        event_type,
        DENSE_RANK() OVER w_all_by_event_at AS event_at_dense_rank,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id, event_type ORDER BY event_at DESC), 3) AS event_at_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor ORDER BY event_at, id), 3) AS event_at_percent_rank,
        ROW_NUMBER() OVER (ORDER BY event_at ASC, id) AS event_at_seq,
        DENSE_RANK() OVER (ORDER BY event_at DESC NULLS FIRST) AS event_at_dense_rank_2,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_2,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at DESC, id))) / 60, 1) AS minutes_since_prev_event_at,
        NTILE(4) OVER (PARTITION BY actor ORDER BY event_at, id) AS event_at_quartile,
        ROW_NUMBER() OVER (ORDER BY event_at DESC NULLS FIRST, id) AS rn
    FROM prepared_events
    WINDOW w_all_by_event_at AS (ORDER BY event_at DESC NULLS FIRST, id)
),
top_rows AS (
    -- Top 5 per group
    SELECT
        id,
        event_type,
        event_at_dense_rank,
        second_event_at,
        event_at_cume_dist,
        event_at_percent_rank,
        event_at_dense_rank_2,
        second_event_at_2,
        minutes_since_prev_event_at,
        event_at_quartile,
        rn
    FROM windowed
    WHERE event_at_seq <= 5
      AND event_at_dense_rank < 9
)
SELECT
    id,
    event_type,
    event_at_dense_rank,
    event_at_cume_dist,
    event_at_percent_rank,
    event_at_dense_rank_2,
    minutes_since_prev_event_at,
    event_at_quartile
FROM top_rows
ORDER BY event_at_dense_rank DESC, id
