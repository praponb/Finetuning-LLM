WITH filtered AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        csat,
        tags,
        CONCAT_WS('-', channel, priority) AS channel_priority_label,
        INITCAP(TRIM(channel)) AS channel_clean,
        SIZE(tags) AS tags_count
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'outage')
      AND opened_at < TIMESTAMP '2025-02-12 10:00:00'
),
with_parent_agent AS (
    -- Attach agent attributes
    SELECT s.*, su.hired_on AS agent_hired_on, su.tier AS agent_tier
    FROM filtered s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.tier = 'T1'
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, reading
FROM (
    SELECT id, tags_count, csat
    FROM with_parent_agent
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric_name IN (tags_count, csat)
) unp
