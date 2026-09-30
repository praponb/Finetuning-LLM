WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug,
        TO_DATE(closed_at) AS closed_at_date
    FROM tickets
    WHERE agent_id <= 7
      AND requester_id > 11
),
without_matches AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.event_type = 'replied'
              AND HOUR(t.event_at) BETWEEN 6 AND 10
        )
),
summary AS (
    -- Summarise per group
    SELECT
        priority,
        channel,
        agent_id,
        ARRAY_SORT(COLLECT_SET(channel)) AS channel_list,
        ROUND(STDDEV(csat), 2) AS stddev_csat,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS cnt
    FROM without_matches
    GROUP BY 1, 2, 3
    HAVING COUNT(*) >= 3
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        priority,
        channel,
        agent_id,
        channel_list,
        stddev_csat,
        ROUND(PERCENT_RANK() OVER (PARTITION BY agent_id ORDER BY stddev_csat DESC, priority, channel, agent_id), 3) AS stddev_csat_percent_rank,
        ROUND(stddev_csat / NULLIF(SUM(stddev_csat) OVER (PARTITION BY priority), 0), 4) AS stddev_csat_share_of_priority,
        NTILE(3) OVER (PARTITION BY agent_id ORDER BY stddev_csat DESC, priority, channel, agent_id) AS stddev_csat_tercile
    FROM summary
),
joined AS (
    -- Join support_agents
    SELECT
        s.priority,
        s.channel,
        s.agent_id,
        s.channel_list,
        s.stddev_csat_share_of_priority,
        s.stddev_csat_tercile,
        su.hired_on AS agent_hired_on
    FROM with_windows s
    LEFT JOIN support_agents su ON su.id = s.agent_id AND su.agent_name IS NOT NULL
)
-- Final projection
SELECT
    priority,
    agent_id,
    channel_list,
    stddev_csat_share_of_priority,
    stddev_csat_tercile,
    agent_hired_on,
    UPPER(TRIM(priority)) AS priority_clean
FROM joined
ORDER BY agent_id
