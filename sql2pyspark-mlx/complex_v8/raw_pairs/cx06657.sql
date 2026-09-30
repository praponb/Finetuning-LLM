WITH filtered AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat
    FROM tickets
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(csat) OVER w_priority_csat_by_csat AS prev_csat,
        NTH_VALUE(closed_at, 2) OVER (ORDER BY opened_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY priority, csat ORDER BY csat DESC), 3) AS csat_percent_rank
    FROM filtered
    WINDOW w_priority_csat_by_csat AS (PARTITION BY priority, csat ORDER BY csat DESC, id)
),
channel_stats AS (
    -- Avg prev_csat per channel
    SELECT channel, AVG(prev_csat) AS avg_prev_csat
    FROM windowed
    GROUP BY channel
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_prev_csat,
        ROUND(s.prev_csat - g.avg_prev_csat, 2) AS prev_csat_minus_avg
    FROM windowed s
    JOIN channel_stats g ON g.channel = s.channel
    WHERE s.prev_csat > g.avg_prev_csat
),
with_children_events AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.csat,
        s.prev_csat,
        s.csat_percent_rank,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type,
        t.event_at
    FROM compared s
    JOIN ticket_events t ON t.ticket_id = s.id
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    closed_at,
    csat,
    prev_csat,
    csat_percent_rank,
    event_id,
    event_actor,
    event_at,
    DATE_TRUNC('HOUR', event_at) AS event_at_hour,
    ABS(csat_percent_rank - 0.3) AS csat_percent_rank_distance
FROM with_children_events
ORDER BY event_at NULLS LAST
