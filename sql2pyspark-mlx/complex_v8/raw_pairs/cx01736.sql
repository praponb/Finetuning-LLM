WITH prepared_events AS (
    -- Filter ticket_events
    SELECT *
    FROM ticket_events
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY event_at, id), 3) AS event_at_cume_dist,
        LAST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_event_type_by_event_at)) / 60, 1) AS minutes_since_prev_event_at,
        NTILE(5) OVER (ORDER BY event_at, id) AS event_at_quintile,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY event_type ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY event_at), 3) AS event_at_percent_rank,
        LEAD(event_at) OVER (PARTITION BY event_type ORDER BY event_at NULLS LAST, id) AS next_event_at,
        DENSE_RANK() OVER w_all_by_event_at_ties AS event_at_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type ORDER BY event_at NULLS LAST, id), 3) AS event_at_cume_dist_2,
        NTILE(3) OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id) AS event_at_tercile
    FROM prepared_events
    WINDOW w_event_type_by_event_at AS (PARTITION BY event_type ORDER BY event_at NULLS LAST, id), w_all_by_event_at_ties AS (ORDER BY event_at)
),
leaders AS (
    -- Top 2 per group
    SELECT *
    FROM sequenced
    WHERE event_at_dense_rank <= 2
      AND event_at_percent_rank >= 0.3
)
SELECT
    id,
    event_type,
    actor,
    event_at_cume_dist,
    last_known_event_at,
    minutes_since_prev_event_at,
    event_at_quintile,
    second_event_at,
    event_at_percent_rank,
    next_event_at,
    event_at_dense_rank,
    event_at_cume_dist_2,
    event_at_tercile,
    HOUR(second_event_at) AS second_event_at_hour
FROM leaders
ORDER BY event_at_quintile DESC NULLS FIRST
