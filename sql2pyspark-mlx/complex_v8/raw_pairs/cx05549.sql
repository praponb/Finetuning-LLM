WITH prepared_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
        DATE_TRUNC('WEEK', event_at) AS event_at_week
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-02-18 11:00:00'
),
combined_details AS (
    -- Join tickets
    SELECT s.*, t.closed_at AS ticket_closed_at
    FROM prepared_events s
    JOIN tickets t ON t.id = s.ticket_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY ticket_id ORDER BY event_at) AS event_at_rank,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (ORDER BY ticket_closed_at DESC NULLS FIRST, id))) / 60, 1) AS minutes_since_prev_event_at,
        LAST_VALUE(ticket_closed_at) OVER (PARTITION BY YEAR(ticket_closed_at) ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ticket_closed_at,
        ROUND((UNIX_TIMESTAMP(event_at_week) - UNIX_TIMESTAMP(LAG(event_at_week) OVER w_all_by_ticket_closed_at)) / 60, 1) AS minutes_since_prev_event_at_week,
        ROW_NUMBER() OVER (ORDER BY ticket_closed_at DESC NULLS FIRST, id) AS rn,
        FIRST_VALUE(event_at_week) IGNORE NULLS OVER (PARTITION BY YEAR(ticket_closed_at) ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_week,
        ROUND(CUME_DIST() OVER w_ticket_id_by_event_at, 3) AS event_at_cume_dist,
        LEAD(event_at_week) OVER (PARTITION BY YEAR(ticket_closed_at) ORDER BY event_at DESC, id) AS next_event_at_week,
        FIRST_VALUE(event_at) OVER (ORDER BY ticket_closed_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at
    FROM combined_details
    WINDOW w_all_by_ticket_closed_at AS (ORDER BY ticket_closed_at DESC NULLS FIRST, id), w_ticket_id_by_event_at AS (PARTITION BY ticket_id ORDER BY event_at, id)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        event_type,
        actor,
        event_type_or_unknown,
        event_at_week,
        minutes_since_prev_event_at,
        minutes_since_prev_event_at_week,
        rn,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY next_event_at_week DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        ROW_NUMBER() OVER (PARTITION BY actor, TO_DATE(event_at_week) ORDER BY minutes_since_prev_event_at_week, id) AS rn_2,
        SIZE(COLLECT_SET(event_type) OVER (PARTITION BY DATE_TRUNC('MONTH', next_event_at_week))) AS distinct_event_type_in_next_event_at_week_month,
        MAX(event_at_cume_dist) OVER w_next_event_at_week_month AS next_event_at_week_month_max_event_at_cume_dist,
        RANK() OVER (ORDER BY next_event_at_week DESC, id) AS next_event_at_week_rank,
        NTILE(5) OVER w_all_by_event_at AS event_at_quintile
    FROM sequenced
    WINDOW w_next_event_at_week_month AS (PARTITION BY DATE_TRUNC('MONTH', next_event_at_week)), w_all_by_event_at AS (ORDER BY event_at DESC, id)
)
SELECT
    id,
    actor,
    event_at_week,
    minutes_since_prev_event_at,
    rn,
    first_known_event_at,
    rn_2,
    distinct_event_type_in_next_event_at_week_month,
    next_event_at_week_month_max_event_at_cume_dist,
    event_at_quintile,
    COALESCE(event_at_quintile, 0) AS event_at_quintile_filled
FROM metrics
WHERE actor IN ('agent', 'customer')
ORDER BY next_event_at_week_month_max_event_at_cume_dist DESC NULLS FIRST
