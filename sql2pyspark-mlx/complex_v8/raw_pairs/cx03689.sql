WITH scoped AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        CONCAT(priority, '#', CAST(requester_id AS STRING)) AS priority_ref,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown,
        ABS(requester_id - 12) AS requester_id_distance
    FROM tickets
    WHERE requester_id >= 3
      AND agent_id <= 3
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        priority_or_unknown,
        agent_id,
        channel,
        ROUND(SUM(csat) / NULLIF(SUM(requester_id), 0), 3) AS csat_to_requester_id_ratio,
        COUNT(DISTINCT agent_id) AS distinct_agent_ids,
        MIN(closed_at) AS first_closed_at
    FROM scoped
    GROUP BY priority_or_unknown, agent_id, channel
),
with_details AS (
    -- Attach agent attributes
    SELECT s.*, su.agent_name, su.tier AS agent_tier
    FROM totals s
    JOIN support_agents su ON su.id = s.agent_id AND su.hired_on BETWEEN DATE '2024-07-09' AND DATE '2025-02-26'
),
with_parent AS (
    -- Attach agent attributes
    SELECT
        s.priority_or_unknown,
        s.agent_id,
        s.channel,
        s.csat_to_requester_id_ratio,
        s.distinct_agent_ids,
        s.first_closed_at,
        su.agent_name AS agent_agent_name
    FROM with_details s
    JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    priority_or_unknown,
    csat_to_requester_id_ratio,
    distinct_agent_ids,
    first_closed_at,
    agent_agent_name,
    SUBSTRING(TRIM(agent_agent_name), 1, 2) AS agent_agent_name_prefix,
    COALESCE(channel, 'UNKNOWN') AS channel_or_unknown
FROM with_parent
