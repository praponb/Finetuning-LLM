WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type = 'assigned'
      AND event_at < TIMESTAMP '2025-03-02 09:00:00'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        event_at,
        actor,
        LEAD(event_at, 2) OVER (PARTITION BY ticket_id, actor ORDER BY event_at ASC, id) AS next2_event_at,
        ROW_NUMBER() OVER (PARTITION BY ticket_id, event_type ORDER BY event_at DESC, id) AS event_at_seq,
        RANK() OVER (ORDER BY event_at DESC) AS event_at_rank,
        NTILE(5) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at DESC, id) AS event_at_quintile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_id, event_type ORDER BY event_at DESC), 3) AS event_at_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id, event_type ORDER BY event_at DESC, id), 3) AS event_at_cume_dist,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at DESC, id))) / 60, 1) AS minutes_since_prev_event_at,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_id, actor ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY ticket_id, actor ORDER BY event_at ASC, id))) / 60, 1) AS minutes_since_prev_event_at_3
    FROM prepared
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_at,
        next2_event_at,
        event_at_seq,
        event_at_rank,
        event_at_percent_rank,
        minutes_since_prev_event_at,
        LAST_VALUE(event_at_percent_rank) OVER (ORDER BY event_at_quintile ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at_percent_rank,
        ROUND(CUME_DIST() OVER (ORDER BY event_at_quintile ASC), 3) AS event_at_quintile_cume_dist,
        SIZE(COLLECT_SET(actor) OVER (PARTITION BY event_at_rank)) AS distinct_actor_in_event_at_rank,
        DENSE_RANK() OVER w_event_at_year_by_event_at_se AS event_at_seq_dense_rank,
        FIRST_VALUE(event_at_rank) IGNORE NULLS OVER (PARTITION BY TO_DATE(next2_event_at) ORDER BY next2_event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_rank,
        MIN(event_at_quintile) OVER (PARTITION BY event_at_seq, YEAR(next2_event_at)) AS event_at_seq_next2_event_at_year_min_event_at_qu,
        ROW_NUMBER() OVER (ORDER BY event_at_quintile ASC, id) AS seq_no
    FROM with_windows
    WINDOW w_event_at_year_by_event_at_se AS (PARTITION BY YEAR(event_at) ORDER BY event_at_seq)
)
-- Final projection
SELECT
    id,
    event_at,
    next2_event_at,
    event_at_rank,
    event_at_percent_rank,
    minutes_since_prev_event_at,
    last_event_at_percent_rank,
    distinct_actor_in_event_at_rank,
    event_at_seq_dense_rank,
    first_known_event_at_rank,
    event_at_seq_next2_event_at_year_min_event_at_qu,
    seq_no,
    GREATEST(event_at_rank, event_at_percent_rank) AS greatest_event_at_rank_event_at_percent_rank,
    MONTH(event_at) AS event_at_month
FROM windowed
ORDER BY event_at_rank DESC
