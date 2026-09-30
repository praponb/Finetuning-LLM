WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type = 'resolved'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_ticket_id_event_at_month_by, 3) AS event_at_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY ticket_id, DATE_TRUNC('MONTH', event_at) ORDER BY event_at DESC, id) AS rn,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at
    FROM scoped
    WINDOW w_ticket_id_event_at_month_by AS (PARTITION BY ticket_id, DATE_TRUNC('MONTH', event_at) ORDER BY event_at DESC, id)
),
actor_stats AS (
    -- Avg minutes_since_prev_event_at per actor
    SELECT actor, AVG(minutes_since_prev_event_at) AS avg_minutes_since_prev_event_at
    FROM with_windows
    GROUP BY actor
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_minutes_since_prev_event_at,
        ROUND(s.minutes_since_prev_event_at - g.avg_minutes_since_prev_event_at, 2) AS minutes_since_prev_event_at_minus_avg
    FROM with_windows s
    JOIN actor_stats g ON g.actor = s.actor
    WHERE s.minutes_since_prev_event_at > g.avg_minutes_since_prev_event_at
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    event_at_cume_dist,
    minutes_since_prev_event_at,
    minutes_since_prev_event_at_minus_avg,
    YEAR(event_at) AS event_at_year
FROM above_average
