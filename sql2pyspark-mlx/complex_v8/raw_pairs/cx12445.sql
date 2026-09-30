WITH joined_ranges AS (
    -- Events that happened while the ticket was open
    SELECT
        e.id,
        e.ticket_id,
        e.event_at,
        e.event_type,
        t.priority,
        t.agent_id,
        ROUND((UNIX_TIMESTAMP(e.event_at) - UNIX_TIMESTAMP(t.opened_at)) / 3600, 2) AS hours_since_open
    FROM ticket_events e
    JOIN tickets t
      ON t.id = e.ticket_id
     AND e.event_at >= t.opened_at
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 2 DAYS)
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        MIN(closed_at) AS first_ticket_closed_at,
        MAX(opened_at) AS last_ticket_opened_at
    FROM tickets
    GROUP BY agent_id
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.first_ticket_closed_at,
        t.last_ticket_opened_at
    FROM joined_ranges s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_all_by_first_ticket_closed_a, 3) AS first_ticket_closed_at_cume_dist,
        DENSE_RANK() OVER (ORDER BY first_ticket_closed_at DESC) AS first_ticket_closed_at_dense_rank,
        ROUND((UNIX_TIMESTAMP(last_ticket_opened_at) - UNIX_TIMESTAMP(LAG(last_ticket_opened_at) OVER (PARTITION BY ticket_id ORDER BY event_at NULLS LAST, id))) / 60, 1) AS minutes_since_prev_last_ticket_opened_at,
        COUNT(*) OVER (PARTITION BY priority, DATE_TRUNC('MONTH', event_at)) AS rows_in_priority_event_at_month
    FROM profiled
    WINDOW w_all_by_first_ticket_closed_a AS (ORDER BY first_ticket_closed_at DESC, id)
),
ticket_id_stats AS (
    -- Max minutes_since_prev_last_ticket_opened_at per ticket_id
    SELECT ticket_id, MAX(minutes_since_prev_last_ticket_opened_at) AS max_minutes_since_prev_last_ticket_opened_at
    FROM metrics
    GROUP BY ticket_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_minutes_since_prev_last_ticket_opened_at,
        ROUND(s.minutes_since_prev_last_ticket_opened_at - g.max_minutes_since_prev_last_ticket_opened_at, 2) AS minutes_since_prev_last_ticket_opened_at_minus_m
    FROM metrics s
    JOIN ticket_id_stats g ON g.ticket_id = s.ticket_id
    WHERE s.minutes_since_prev_last_ticket_opened_at = g.max_minutes_since_prev_last_ticket_opened_at
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    priority,
    hours_since_open,
    first_ticket_closed_at,
    first_ticket_closed_at_cume_dist,
    first_ticket_closed_at_dense_rank,
    minutes_since_prev_last_ticket_opened_at,
    rows_in_priority_event_at_month,
    max_minutes_since_prev_last_ticket_opened_at,
    minutes_since_prev_last_ticket_opened_at_minus_m,
    CONCAT_WS(' / ', event_type, priority) AS event_type_priority_label
FROM above_average
WHERE TO_DATE(event_at) < DATE '2025-02-12'
