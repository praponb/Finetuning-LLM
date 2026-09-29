WITH filtered_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        TO_DATE(event_at) AS event_at_date,
        LENGTH(TRIM(actor)) AS actor_len,
        event_at + INTERVAL 15 MINUTES AS event_at_plus_15m
    FROM ticket_events
    WHERE event_type IN ('created', 'resolved')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_at,
        actor,
        actor_len,
        event_at_plus_15m,
        ROUND(actor_len - AVG(actor_len) OVER (PARTITION BY ticket_id, DATE_TRUNC('MONTH', event_at)), 2) AS actor_len_vs_ticket_id_event_at_month_avg,
        ROUND(actor_len / NULLIF(SUM(actor_len) OVER w_ticket_id_event_at_month, 0), 4) AS actor_len_share_of_ticket_id_event_at_month,
        COUNT(*) OVER (PARTITION BY ticket_id, DATE_TRUNC('MONTH', event_at)) AS rows_in_ticket_id_event_at_month
    FROM filtered_events
    WINDOW w_ticket_id_event_at_month AS (PARTITION BY ticket_id, DATE_TRUNC('MONTH', event_at))
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_windows
    WHERE rows_in_ticket_id_event_at_month > (SELECT AVG(rows_in_ticket_id_event_at_month) FROM with_windows)
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM with_activity
    WHERE actor_len < (SELECT MAX(actor_len) FROM with_activity)
)
SELECT
    id,
    event_at,
    actor,
    event_at_plus_15m,
    actor_len_vs_ticket_id_event_at_month_avg,
    actor_len_share_of_ticket_id_event_at_month,
    rows_in_ticket_id_event_at_month
FROM qualified
ORDER BY id DESC
