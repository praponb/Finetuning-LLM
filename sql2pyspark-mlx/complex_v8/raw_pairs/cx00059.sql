WITH filtered_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor IN ('customer', 'system')
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        FIRST_VALUE(event_at) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at), event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        FIRST_VALUE(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at_2,
        ROUND(CUME_DIST() OVER (ORDER BY event_at DESC, id), 3) AS event_at_cume_dist,
        NTILE(5) OVER (ORDER BY event_at DESC, id) AS event_at_quintile,
        RANK() OVER w_ticket_id_actor_by_event_at AS event_at_rank,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_all_by_event_at)) / 60, 1) AS minutes_since_prev_event_at,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_id, actor ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        NTILE(10) OVER (PARTITION BY ticket_id ORDER BY event_at ASC, id) AS event_at_decile,
        DENSE_RANK() OVER w_ticket_id_by_event_at AS event_at_dense_rank,
        RANK() OVER w_all_by_event_at AS event_at_rank_2,
        LEAD(event_at, 3) OVER (PARTITION BY DATE_TRUNC('MONTH', event_at), event_type ORDER BY event_at, id) AS next3_event_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY event_at DESC), 3) AS event_at_percent_rank,
        LEAD(event_at) OVER w_event_at_month_event_type_by_2 AS next_event_at,
        LAST_VALUE(event_at) OVER (PARTITION BY ticket_id, actor ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        ROUND(PERCENT_RANK() OVER w_event_at_month_ticket_id_by, 3) AS event_at_percent_rank_2
    FROM filtered_events
    WINDOW w_ticket_id_actor_by_event_at AS (PARTITION BY ticket_id, actor ORDER BY event_at DESC, id), w_all_by_event_at AS (ORDER BY event_at DESC, id), w_ticket_id_by_event_at AS (PARTITION BY ticket_id ORDER BY event_at ASC, id), w_event_at_month_event_type_by_2 AS (PARTITION BY DATE_TRUNC('MONTH', event_at), event_type ORDER BY event_at, id), w_event_at_month_ticket_id_by AS (PARTITION BY DATE_TRUNC('MONTH', event_at), ticket_id ORDER BY event_at DESC)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        first_event_at,
        first_event_at_2,
        event_at_cume_dist,
        event_at_quintile,
        event_at_rank,
        minutes_since_prev_event_at,
        first_known_event_at,
        event_at_decile,
        event_at_rank_2,
        event_at_percent_rank,
        next_event_at,
        last_event_at,
        event_at_percent_rank_2,
        ROUND(AVG(minutes_since_prev_event_at) OVER w_event_at_quintile_next_event, 2) AS event_at_quintile_next_event_at_month_avg_minute,
        NTILE(3) OVER (PARTITION BY DATE_TRUNC('MONTH', first_event_at) ORDER BY event_at_rank_2, id) AS event_at_rank_2_tercile,
        NTILE(3) OVER w_all_by_first_event_at AS first_event_at_tercile
    FROM windowed
    WINDOW w_event_at_quintile_next_event AS (PARTITION BY event_at_quintile, MONTH(next_event_at)), w_all_by_first_event_at AS (ORDER BY first_event_at, id)
)
SELECT
    id,
    event_type,
    actor,
    first_event_at,
    first_event_at_2,
    event_at_cume_dist,
    event_at_quintile,
    event_at_rank,
    minutes_since_prev_event_at,
    first_known_event_at,
    event_at_decile,
    event_at_rank_2,
    event_at_percent_rank,
    event_at_percent_rank_2,
    first_event_at_tercile,
    LEAST(event_at_decile, event_at_cume_dist) AS least_event_at_decile_event_at_cume_dist,
    ROUND(first_event_at_tercile / NULLIF(event_at_cume_dist, 0), 3) AS first_event_at_tercile_per_event_at_cume_dist
FROM with_windows
ORDER BY first_event_at_tercile NULLS LAST, actor DESC
