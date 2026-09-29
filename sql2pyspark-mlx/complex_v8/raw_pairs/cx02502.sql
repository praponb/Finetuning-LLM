WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        days_since_event_at,
        ROUND(AVG(days_since_event_at) OVER (PARTITION BY actor, ticket_id), 2) AS actor_ticket_id_avg_days_since_event_at,
        ROUND(days_since_event_at - AVG(days_since_event_at) OVER w_actor, 2) AS days_since_event_at_vs_actor_avg,
        ROW_NUMBER() OVER (ORDER BY event_at DESC, id) AS event_at_seq,
        COUNT(*) OVER w_actor AS rows_in_actor,
        LEAD(days_since_event_at) OVER (ORDER BY days_since_event_at DESC NULLS FIRST, id) AS next_days_since_event_at,
        SUM(days_since_event_at) OVER (PARTITION BY actor, ticket_id) AS actor_ticket_id_sum_days_since_event_at,
        RANK() OVER (PARTITION BY ticket_id ORDER BY days_since_event_at) AS days_since_event_at_rank,
        LAG(days_since_event_at, 1, 0) OVER (PARTITION BY actor ORDER BY event_at DESC, id) AS prev_days_since_event_at,
        MAX(days_since_event_at) OVER (PARTITION BY actor ORDER BY event_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS days_since_event_at_max_3,
        SUM(days_since_event_at) OVER (PARTITION BY actor) AS actor_sum_days_since_event_at
    FROM valid
    WINDOW w_actor AS (PARTITION BY actor)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(days_since_event_at) OVER (PARTITION BY event_at_seq ORDER BY days_since_event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_days_since_event_at,
        ROUND(next_days_since_event_at - AVG(next_days_since_event_at) OVER w_actor_event_at_month, 2) AS next_days_since_event_at_vs_actor_event_at_month,
        SUM(days_since_event_at_max_3) OVER (PARTITION BY actor, DATE_TRUNC('MONTH', event_at)) AS actor_event_at_month_sum_days_since_event_at_max,
        FIRST_VALUE(next_days_since_event_at) IGNORE NULLS OVER (PARTITION BY event_type ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_next_days_since_event_at,
        MAX(rows_in_actor) OVER (PARTITION BY event_type ORDER BY event_at NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS rows_in_actor_max_2,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type ORDER BY event_at NULLS LAST, id), 3) AS event_at_cume_dist
    FROM ranked
    WINDOW w_actor_event_at_month AS (PARTITION BY actor, DATE_TRUNC('MONTH', event_at))
)
SELECT DISTINCT actor, next_days_since_event_at
FROM with_trends
ORDER BY next_days_since_event_at
