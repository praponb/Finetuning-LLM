WITH scoped_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        COALESCE(channel, 'UNKNOWN') AS channel_or_unknown
    FROM tickets
    WHERE priority = 'P4'
      AND agent_id >= 2
),
line_level AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.channel,
        s.csat,
        t.id AS event_id,
        t.event_at,
        t.event_type
    FROM scoped_tickets s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        event_id,
        event_at,
        event_type,
        MAX(requester_id) OVER (PARTITION BY channel, agent_id ORDER BY event_at, id, event_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS requester_id_max_3,
        DENSE_RANK() OVER (PARTITION BY channel, agent_id ORDER BY event_at, id, event_id) AS event_at_dense_rank
    FROM line_level
),
priority_stats AS (
    -- Median csat per priority
    SELECT priority, PERCENTILE_APPROX(csat, 0.5) AS median_csat
    FROM sequenced
    GROUP BY priority
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_csat, ROUND(s.csat - g.median_csat, 2) AS csat_minus_median
    FROM sequenced s
    JOIN priority_stats g ON g.priority = s.priority
    WHERE s.csat >= g.median_csat
),
with_parent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.channel,
        s.csat,
        s.event_id,
        s.event_at,
        s.event_type,
        s.event_at_dense_rank,
        s.median_csat,
        s.csat_minus_median,
        su.tier AS agent_tier,
        t.actor AS event_actor,
        t.event_at AS event_event_at
    FROM vs_group s
    JOIN support_agents su ON su.id = s.agent_id
    JOIN ticket_events t ON t.id = s.event_id
    WHERE su.tier IS NOT NULL
),
sequenced_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(opened_at, 2) OVER (PARTITION BY event_at_dense_rank ORDER BY csat ASC NULLS LAST, id, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_at,
        SUM(csat) OVER (PARTITION BY event_at_dense_rank ORDER BY csat ASC NULLS LAST, id, event_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS csat_sum_7,
        ROUND(AVG(csat) OVER (PARTITION BY event_at_dense_rank ORDER BY csat ASC NULLS LAST, id, event_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS csat_avg_7,
        SIZE(COLLECT_SET(event_type) OVER (PARTITION BY event_actor, event_id)) AS distinct_event_type_in_event_actor_event_id
    FROM with_parent
)
-- Final projection
SELECT
    id,
    agent_id,
    opened_at,
    priority,
    channel,
    csat,
    event_id,
    event_at_dense_rank,
    median_csat,
    csat_minus_median,
    agent_tier,
    event_actor,
    distinct_event_type_in_event_actor_event_id,
    opened_at + INTERVAL 30 MINUTES AS opened_at_plus_30m
FROM sequenced_2
WHERE event_event_at < TIMESTAMP '2025-03-02 09:00:00'
ORDER BY priority DESC
