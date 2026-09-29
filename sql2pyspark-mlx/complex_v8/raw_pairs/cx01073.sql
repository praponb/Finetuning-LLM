WITH base AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE SIZE(tags) >= 2
      AND agent_id <= 4
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT(channel, '#', CAST(agent_id AS STRING)) AS channel_ref,
        GREATEST(csat, requester_id) AS greatest_csat_requester_id,
        requester_id + csat AS requester_id_plus_csat
    FROM base
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE t.priority IS NOT NULL) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_priority_count,
        (SELECT MIN(t.closed_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS first_ticket_closed_at,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count
    FROM annotated s
),
joined AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.csat,
        s.tags,
        s.channel_ref,
        s.greatest_csat_requester_id,
        s.first_ticket_closed_at,
        s.ticket_count,
        su.agent_name
    FROM with_stats s
    LEFT JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    agent_id,
    csat,
    COALESCE(priority, 'ALL') AS priority,
    MAX(closed_at) AS last_closed_at,
    COUNT(*) AS row_count
FROM joined
GROUP BY GROUPING SETS ((agent_id, csat, priority), (agent_id), ())
ORDER BY agent_id NULLS LAST, csat NULLS LAST, priority NULLS LAST
