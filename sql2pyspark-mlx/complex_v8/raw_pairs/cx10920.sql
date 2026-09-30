WITH prepared AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-03-08 09:00:00'
      AND ticket_id >= 50
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at,
        ROW_NUMBER() OVER (PARTITION BY actor ORDER BY event_at ASC, id) AS event_at_seq,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY actor ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        NTILE(4) OVER (PARTITION BY actor ORDER BY event_at ASC, id) AS event_at_quartile
    FROM prepared
),
top_per_group AS (
    -- Top 2 per group
    SELECT *
    FROM sequenced
    WHERE event_at_seq <= 2
      AND minutes_since_prev_event_at <= 3500.0
),
actor_stats AS (
    -- Avg event_at_quartile per actor
    SELECT actor, AVG(event_at_quartile) AS avg_event_at_quartile
    FROM top_per_group
    GROUP BY actor
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_event_at_quartile,
        ROUND(s.event_at_quartile - g.avg_event_at_quartile, 2) AS event_at_quartile_minus_avg
    FROM top_per_group s
    JOIN actor_stats g ON g.actor = s.actor
    WHERE s.event_at_quartile > g.avg_event_at_quartile
)
SELECT *
FROM (
    SELECT event_at_seq, event_type, actor, avg_event_at_quartile
    FROM compared
) src
PIVOT (
    COUNT(avg_event_at_quartile) AS count_v, SUM(avg_event_at_quartile) AS sum_v
    FOR actor IN ('agent', 'customer')
)
ORDER BY event_at_seq, event_type
