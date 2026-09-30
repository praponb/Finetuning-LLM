WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        priority,
        channel,
        tags
    FROM tickets
    WHERE priority IN ('P3', 'P4')
      AND HOUR(opened_at) BETWEEN 7 AND 12
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        priority,
        agent_id,
        channel,
        COUNT(*) AS cnt,
        COUNT(*) FILTER (WHERE channel IN ('chat', 'web')) AS n_channel_in_chat_web,
        ARRAY_SORT(COLLECT_SET(priority)) AS priority_list,
        SUM(CASE WHEN channel = 'phone' THEN 1 ELSE 0 END) AS n_channel_phone,
        COUNT(*) FILTER (WHERE priority = 'P3') AS n_priority_p3
    FROM recent
    GROUP BY 1, 2, 3
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(channel), 1, 3) AS channel_prefix,
        SIZE(priority_list) AS priority_list_count
    FROM per_group
    WHERE n_channel_in_chat_web < 8
),
enriched_agent AS (
    -- Attach agent attributes
    SELECT
        s.priority,
        s.agent_id,
        s.channel,
        s.cnt,
        s.priority_list,
        s.n_channel_phone,
        s.channel_prefix,
        s.priority_list_count,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier,
        su.agent_name
    FROM scored s
    LEFT JOIN support_agents su ON su.id = s.agent_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT priority, agent_id, metric_name, metric_value
FROM (
    SELECT priority, agent_id, n_channel_phone, priority_list_count
    FROM enriched_agent
) src
UNPIVOT (
    metric_value FOR metric_name IN (n_channel_phone, priority_list_count)
) unp
ORDER BY priority, agent_id, metric_name
