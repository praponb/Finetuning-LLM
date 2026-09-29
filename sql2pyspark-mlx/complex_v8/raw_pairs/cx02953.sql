WITH recent_tickets AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        channel,
        LENGTH(TRIM(channel)) AS channel_len
    FROM tickets
    WHERE csat BETWEEN 1 AND 4
      AND priority = 'P1'
),
priority_stats AS (
    -- Median channel_len per priority
    SELECT priority, PERCENTILE_APPROX(channel_len, 0.5) AS median_channel_len
    FROM recent_tickets
    GROUP BY priority
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_channel_len,
        ROUND(s.channel_len - g.median_channel_len, 2) AS channel_len_minus_median
    FROM recent_tickets s
    JOIN priority_stats g ON g.priority = s.priority
    WHERE s.channel_len >= g.median_channel_len
),
priority_stats_2 AS (
    -- Median median_channel_len per priority
    SELECT priority, PERCENTILE_APPROX(median_channel_len, 0.5) AS median_median_channel_len
    FROM above_average
    GROUP BY priority
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_median_channel_len,
        ROUND(s.median_channel_len - g.median_median_channel_len, 2) AS median_channel_len_minus_median
    FROM above_average s
    JOIN priority_stats_2 g ON g.priority = s.priority
    WHERE s.median_channel_len >= g.median_median_channel_len
),
joined AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.priority,
        s.channel,
        s.channel_len,
        s.median_channel_len,
        s.channel_len_minus_median,
        s.median_median_channel_len,
        s.median_channel_len_minus_median,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier,
        su.agent_name
    FROM vs_group s
    JOIN support_agents su ON su.id = s.agent_id
),
detail_rows AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.channel_len,
        s.channel_len_minus_median,
        s.median_median_channel_len,
        s.agent_hired_on,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type,
        t.event_at
    FROM joined s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id
),
event_actor_stats AS (
    -- Avg channel_len per event_actor
    SELECT event_actor, AVG(channel_len) AS avg_channel_len
    FROM detail_rows
    GROUP BY event_actor
),
vs_group_2 AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_channel_len,
        ROUND(s.channel_len - g.avg_channel_len, 2) AS channel_len_minus_avg
    FROM detail_rows s
    JOIN event_actor_stats g ON g.event_actor = s.event_actor
    WHERE s.channel_len > g.avg_channel_len
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        channel_len,
        channel_len_minus_median,
        median_median_channel_len,
        event_id,
        event_actor,
        event_type,
        event_at,
        avg_channel_len,
        channel_len_minus_avg,
        CONCAT_WS(': ', event_actor, event_type) AS event_actor_event_type_label,
        SUBSTRING(TRIM(event_actor), 1, 2) AS event_actor_prefix,
        CONCAT(event_actor, '#', CAST(event_id AS STRING)) AS event_actor_ref
    FROM vs_group_2
    WHERE event_at >= TIMESTAMP '2025-02-06 12:00:00'
)
SELECT *
FROM (
    SELECT channel_len, event_at, event_type, avg_channel_len
    FROM labeled
) src
PIVOT (
    MIN(avg_channel_len)
    FOR event_type IN ('replied', 'reopened')
)
ORDER BY channel_len, event_at
