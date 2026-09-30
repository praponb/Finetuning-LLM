WITH base AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        TO_DATE(event_at) AS event_at_date,
        DATE_TRUNC('WEEK', event_at) AS event_at_week
    FROM ticket_events
    WHERE ticket_id <= 9
      AND actor = 'customer'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(event_at_week) - UNIX_TIMESTAMP(LAG(event_at_week) OVER (PARTITION BY event_type, TO_DATE(event_at) ORDER BY event_at_week ASC, id))) / 60, 1) AS minutes_since_prev_event_at_week,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (ORDER BY event_at_date DESC, id))) / 60, 1) AS minutes_since_prev_event_at
    FROM base
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        DATEDIFF(event_at_date, LAG(event_at_date) OVER (PARTITION BY actor ORDER BY event_at NULLS LAST, id)) AS days_since_prev_event_at_date,
        LAG(minutes_since_prev_event_at, 3) OVER w_actor_by_event_at AS prev3_minutes_since_prev_event_at,
        ROUND(minutes_since_prev_event_at - AVG(minutes_since_prev_event_at) OVER (PARTITION BY TO_DATE(event_at)), 2) AS minutes_since_prev_event_at_vs_event_at_to_date_
    FROM windowed
    WINDOW w_actor_by_event_at AS (PARTITION BY actor ORDER BY event_at NULLS LAST, id)
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at
    FROM sequenced s
),
windowed_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY event_type, actor ORDER BY event_at DESC, id) AS event_at_seq,
        COUNT(*) OVER (PARTITION BY ticket_id, event_type) AS rows_in_ticket_id_event_type,
        ROUND(AVG(minutes_since_prev_event_at) OVER (PARTITION BY ticket_id, event_type ORDER BY minutes_since_prev_event_at_vs_event_at_to_date_ DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS minutes_since_prev_event_at_avg_6_prior,
        DENSE_RANK() OVER w_actor_event_at_date_month_by AS minutes_since_prev_event_at_week_dense_rank,
        SUM(minutes_since_prev_event_at) OVER (PARTITION BY event_type, actor ORDER BY event_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS minutes_since_prev_event_at_sum_2_prior,
        ROUND(minutes_since_prev_event_at - AVG(minutes_since_prev_event_at) OVER (PARTITION BY ticket_id, event_type), 2) AS minutes_since_prev_event_at_vs_ticket_id_event_t
    FROM with_stats
    WINDOW w_actor_event_at_date_month_by AS (PARTITION BY actor, DATE_TRUNC('MONTH', event_at_date) ORDER BY minutes_since_prev_event_at_week, id)
),
windowed_3 AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(event_at_seq) OVER (PARTITION BY minutes_since_prev_event_at_week_dense_rank ORDER BY event_at_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_at_seq,
        ROUND(CUME_DIST() OVER w_actor_by_prev3_minutes_since, 3) AS prev3_minutes_since_prev_event_at_cume_dist
    FROM windowed_2
    WINDOW w_actor_by_prev3_minutes_since AS (PARTITION BY actor ORDER BY prev3_minutes_since_prev_event_at ASC, id)
)
SELECT
    id,
    event_type,
    event_at_date,
    event_at_week,
    minutes_since_prev_event_at_week,
    minutes_since_prev_event_at,
    days_since_prev_event_at_date,
    prev3_minutes_since_prev_event_at,
    minutes_since_prev_event_at_vs_event_at_to_date_,
    last_event_at,
    event_at_seq,
    minutes_since_prev_event_at_avg_6_prior,
    minutes_since_prev_event_at_week_dense_rank,
    minutes_since_prev_event_at_vs_ticket_id_event_t,
    running_event_at_seq,
    prev3_minutes_since_prev_event_at_cume_dist
FROM windowed_3
ORDER BY event_at_seq DESC
