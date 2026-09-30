WITH base_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND actor IN ('agent', 'system')
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        DENSE_RANK() OVER (PARTITION BY TO_DATE(event_at) ORDER BY event_at DESC NULLS FIRST) AS event_at_dense_rank,
        NTILE(3) OVER (PARTITION BY TO_DATE(event_at), event_type ORDER BY event_at DESC, id) AS event_at_tercile,
        ROW_NUMBER() OVER (PARTITION BY event_type, actor ORDER BY event_at DESC, id) AS event_at_seq,
        ROUND(PERCENT_RANK() OVER (PARTITION BY TO_DATE(event_at) ORDER BY event_at DESC NULLS FIRST), 3) AS event_at_percent_rank,
        LEAD(event_at) OVER w_event_at_to_date_by_event_at_2 AS next_event_at,
        ROUND(CUME_DIST() OVER (PARTITION BY actor, TO_DATE(event_at) ORDER BY event_at DESC, id), 3) AS event_at_cume_dist,
        NTILE(3) OVER (PARTITION BY MONTH(event_at), actor ORDER BY event_at DESC, id) AS event_at_tercile_2,
        FIRST_VALUE(event_at) OVER (PARTITION BY TO_DATE(event_at), event_type ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY TO_DATE(event_at) ORDER BY event_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_event_at_month_actor_by_even)) / 60, 1) AS minutes_since_prev_event_at,
        DENSE_RANK() OVER w_event_at_to_date_event_type AS event_at_dense_rank_2
    FROM base_events
    WINDOW w_event_at_to_date_by_event_at_2 AS (PARTITION BY TO_DATE(event_at) ORDER BY event_at DESC NULLS FIRST, id), w_event_at_month_actor_by_even AS (PARTITION BY MONTH(event_at), actor ORDER BY event_at DESC, id), w_event_at_to_date_event_type AS (PARTITION BY TO_DATE(event_at), event_type ORDER BY event_at DESC, id)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(event_at_cume_dist) OVER (PARTITION BY event_at_tercile, event_at_seq ORDER BY event_at_cume_dist NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS event_at_cume_dist_sum_1_prior,
        ROUND(AVG(event_at_tercile) OVER (PARTITION BY event_at_dense_rank_2, ticket_id), 2) AS event_at_dense_rank_2_ticket_id_avg_event_at_ter,
        ROUND(AVG(event_at_tercile_2) OVER (ORDER BY event_at_cume_dist DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS event_at_tercile_2_avg_2_prior,
        MAX(event_at_tercile_2) OVER (PARTITION BY ticket_id ORDER BY event_at_dense_rank DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_event_at_tercile_2_so_far,
        ROUND(event_at_seq / NULLIF(SUM(event_at_seq) OVER (PARTITION BY event_at_dense_rank_2, ticket_id), 0), 4) AS event_at_seq_share_of_event_at_dense_rank_2_tick,
        NTILE(5) OVER (PARTITION BY ticket_id ORDER BY event_at_dense_rank DESC NULLS FIRST, id) AS event_at_dense_rank_quintile,
        RANK() OVER (PARTITION BY ticket_id ORDER BY event_at_dense_rank DESC NULLS FIRST, id) AS event_at_dense_rank_rank
    FROM windowed
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    event_at_dense_rank,
    event_at_tercile,
    event_at_seq,
    next_event_at,
    event_at_tercile_2,
    first_event_at,
    first_known_event_at,
    minutes_since_prev_event_at,
    event_at_cume_dist_sum_1_prior,
    event_at_dense_rank_2_ticket_id_avg_event_at_ter,
    max_event_at_tercile_2_so_far,
    event_at_seq_share_of_event_at_dense_rank_2_tick,
    event_at_dense_rank_quintile,
    event_at_dense_rank_rank,
    ABS(event_at_tercile_2 - 7) AS event_at_tercile_2_distance,
    CONCAT(event_type, '#', CAST(event_at_dense_rank AS STRING)) AS event_type_ref
FROM with_trends
ORDER BY event_at_dense_rank DESC, event_at_dense_rank_quintile NULLS LAST, id
LIMIT 10
