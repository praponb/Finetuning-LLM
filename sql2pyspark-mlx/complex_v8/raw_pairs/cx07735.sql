WITH base_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id >= 24
      AND event_type = 'replied'
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_id, actor ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROW_NUMBER() OVER (PARTITION BY ticket_id, actor ORDER BY event_at NULLS LAST, id) AS rn,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id, actor ORDER BY event_at NULLS LAST), 3) AS event_at_cume_dist,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id ORDER BY event_at DESC), 3) AS event_at_cume_dist_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_id ORDER BY event_at DESC), 3) AS event_at_percent_rank,
        LAST_VALUE(event_at) OVER (PARTITION BY ticket_id, actor ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        LEAD(event_at) OVER w_ticket_id_actor_by_event_at_2 AS next_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_ticket_id_by_event_at)) / 60, 1) AS minutes_since_prev_event_at,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_2,
        ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id) AS rn_2,
        LAG(event_at) OVER (ORDER BY event_at, id) AS prev_event_at
    FROM base_events
    WINDOW w_ticket_id_actor_by_event_at_3 AS (PARTITION BY ticket_id, actor ORDER BY event_at NULLS LAST), w_ticket_id_actor_by_event_at_2 AS (PARTITION BY ticket_id, actor ORDER BY event_at NULLS LAST, id), w_ticket_id_by_event_at AS (PARTITION BY ticket_id ORDER BY event_at DESC, id)
),
sequenced_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(event_at_cume_dist) OVER (PARTITION BY TO_DATE(second_event_at)) AS second_event_at_to_date_max_event_at_cume_dist,
        ROUND(event_at_percent_rank - AVG(event_at_percent_rank) OVER (PARTITION BY TO_DATE(second_event_at)), 2) AS event_at_percent_rank_vs_second_event_at_to_date,
        ROUND(rn_2 - AVG(rn_2) OVER (PARTITION BY TO_DATE(second_event_at)), 2) AS rn_2_vs_second_event_at_to_date_avg,
        NTH_VALUE(minutes_since_prev_event_at, 2) OVER (PARTITION BY TO_DATE(second_event_at_2), event_type ORDER BY minutes_since_prev_event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_minutes_since_prev_event_at
    FROM sequenced
),
stats AS (
    -- Group-level totals
    SELECT
        rn,
        YEAR(next_event_at) AS next_event_at_year,
        actor,
        COUNT(DISTINCT ticket_id) AS distinct_ticket_ids,
        MAX(event_at) AS last_event_at_2,
        SUM(minutes_since_prev_event_at) AS total_minutes_since_prev_event_at
    FROM sequenced_2
    GROUP BY 1, 2, 3
    HAVING SUM(event_at_cume_dist) > 0.4
)
-- Final projection
SELECT
    rn,
    next_event_at_year,
    actor,
    distinct_ticket_ids,
    last_event_at_2,
    total_minutes_since_prev_event_at
FROM stats
WHERE distinct_ticket_ids > 4
ORDER BY last_event_at_2 DESC, rn, next_event_at_year, actor
LIMIT 5
