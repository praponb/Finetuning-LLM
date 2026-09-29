WITH filtered_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type = 'reopened'
      AND ticket_id IS NOT NULL
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY event_type ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_event_type_actor_by_event_at)) / 60, 1) AS minutes_since_prev_event_at,
        LAG(event_at, 2) OVER (PARTITION BY event_type, actor ORDER BY event_at, id) AS prev2_event_at,
        NTILE(4) OVER (PARTITION BY event_type ORDER BY event_at DESC NULLS FIRST, id) AS event_at_quartile,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at ASC, id))) / 60, 1) AS minutes_since_prev_event_at_2,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type ORDER BY event_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at
    FROM filtered_events
    WINDOW w_event_type_actor_by_event_at AS (PARTITION BY event_type, actor ORDER BY event_at, id)
),
actor_stats AS (
    -- Avg minutes_since_prev_event_at_2 per actor
    SELECT actor, AVG(minutes_since_prev_event_at_2) AS avg_minutes_since_prev_event_at_2
    FROM sequenced
    GROUP BY actor
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_minutes_since_prev_event_at_2,
        ROUND(s.minutes_since_prev_event_at_2 - g.avg_minutes_since_prev_event_at_2, 2) AS minutes_since_prev_event_at_2_minus_avg
    FROM sequenced s
    JOIN actor_stats g ON g.actor = s.actor
    WHERE s.minutes_since_prev_event_at_2 > g.avg_minutes_since_prev_event_at_2
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count
    FROM above_average s
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(actor, 'ALL') AS actor,
    ticket_id,
    event_count,
    GROUPING(actor) AS is_total_actor,
    SUM(CASE WHEN event_type = 'resolved' THEN 1 ELSE 0 END) AS n_event_type_resolved
FROM profiled
GROUP BY GROUPING SETS ((actor, ticket_id, event_count), (actor), (ticket_id))
ORDER BY actor NULLS LAST, ticket_id NULLS LAST, event_count NULLS LAST
