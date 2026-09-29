WITH valid_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id <= 6
      AND HOUR(event_at) BETWEEN 7 AND 13
),
joined AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.closed_at AS ticket_closed_at
    FROM valid_events s
    JOIN tickets t ON t.id = s.ticket_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        event_at,
        event_type,
        actor,
        ROUND((UNIX_TIMESTAMP(ticket_closed_at) - UNIX_TIMESTAMP(LAG(ticket_closed_at) OVER (PARTITION BY ticket_id, actor ORDER BY ticket_closed_at DESC, id))) / 60, 1) AS minutes_since_prev_ticket_closed_at,
        RANK() OVER (PARTITION BY event_type, ticket_id ORDER BY ticket_closed_at) AS ticket_closed_at_rank,
        LAG(ticket_closed_at, 3) OVER (PARTITION BY event_type, ticket_id ORDER BY ticket_closed_at, id) AS prev3_ticket_closed_at,
        LAG(event_at) OVER (PARTITION BY event_type, actor ORDER BY ticket_closed_at ASC, id) AS prev_event_at,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', ticket_closed_at) ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        NTH_VALUE(ticket_closed_at, 2) OVER (PARTITION BY event_type, ticket_id ORDER BY ticket_closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_closed_at,
        NTH_VALUE(ticket_closed_at, 2) OVER (PARTITION BY event_type, actor ORDER BY ticket_closed_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ticket_closed_at_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor, ticket_id ORDER BY ticket_closed_at DESC, id), 3) AS ticket_closed_at_percent_rank,
        RANK() OVER (PARTITION BY ticket_id, actor ORDER BY ticket_closed_at) AS ticket_closed_at_rank_2,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', ticket_closed_at) ORDER BY event_at), 3) AS event_at_cume_dist,
        LAG(ticket_closed_at) OVER (PARTITION BY event_type, actor ORDER BY ticket_closed_at ASC, id) AS prev_ticket_closed_at,
        LAG(ticket_closed_at) OVER w_ticket_id_actor_by_ticket_cl_3 AS prev_ticket_closed_at_2,
        RANK() OVER w_event_type_actor_by_ticket_c_3 AS ticket_closed_at_rank_3,
        ROUND(PERCENT_RANK() OVER w_ticket_id_actor_by_ticket_cl, 3) AS ticket_closed_at_percent_rank_2
    FROM joined
    WINDOW w_ticket_id_actor_by_ticket_cl_3 AS (PARTITION BY ticket_id, actor ORDER BY ticket_closed_at, id), w_event_type_actor_by_ticket_c_3 AS (PARTITION BY event_type, actor ORDER BY ticket_closed_at ASC), w_ticket_id_actor_by_ticket_cl AS (PARTITION BY ticket_id, actor ORDER BY ticket_closed_at DESC, id)
),
per_group AS (
    -- Summarise per group
    SELECT
        ticket_closed_at_rank_2,
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        ticket_closed_at_rank_3,
        MAX(prev3_ticket_closed_at) AS last_prev3_ticket_closed_at,
        COUNT(*) AS cnt
    FROM ranked
    GROUP BY ticket_closed_at_rank_2, DATE_TRUNC('MONTH', event_at), ticket_closed_at_rank_3
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(cnt) OVER (ORDER BY cnt ASC, ticket_closed_at_rank_2, event_at_month, ticket_closed_at_rank_3 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_cnt_so_far,
        SUM(ticket_closed_at_rank_2) OVER (PARTITION BY ticket_closed_at_rank_2) AS ticket_closed_at_rank_2_sum_ticket_closed_at_ran
    FROM per_group
)
SELECT
    ticket_closed_at_rank_2,
    event_at_month,
    ticket_closed_at_rank_3,
    last_prev3_ticket_closed_at,
    min_cnt_so_far,
    ticket_closed_at_rank_2_sum_ticket_closed_at_ran,
    FLOOR(cnt / 2) * 2 AS cnt_bucket,
    DATE_TRUNC('HOUR', event_at_month) AS event_at_month_hour
FROM with_trends
WHERE event_at_month < TIMESTAMP '2025-02-05 09:00:00'
ORDER BY ticket_closed_at_rank_2 DESC, event_at_month, ticket_closed_at_rank_3
LIMIT 20
