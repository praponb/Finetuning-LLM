WITH filtered_tickets AS (
    -- Scope the tickets rows
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
    WHERE (opened_at < TIMESTAMP '2025-03-02 09:00:00' OR closed_at >= TIMESTAMP '2025-01-27 00:00:00')
),
with_details AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.channel,
        s.tags,
        su.agent_name,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier
    FROM filtered_tickets s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE LENGTH(TRIM(su.agent_name)) >= 5
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        opened_at,
        priority,
        channel,
        tags,
        agent_name,
        agent_hired_on,
        agent_tier,
        RANK() OVER (PARTITION BY channel, priority ORDER BY agent_hired_on, id) AS agent_hired_on_rank,
        ROW_NUMBER() OVER w_channel_priority_by_agent_hi AS rn,
        LAG(opened_at) OVER (PARTITION BY priority, agent_tier ORDER BY opened_at DESC, id) AS prev_opened_at,
        LEAD(opened_at) OVER w_priority_agent_tier_by_opene AS next_opened_at
    FROM with_details
    WINDOW w_channel_priority_by_agent_hi AS (PARTITION BY channel, priority ORDER BY agent_hired_on, id), w_priority_agent_tier_by_opene AS (PARTITION BY priority, agent_tier ORDER BY opened_at DESC, id)
)
SELECT
    channel,
    agent_tier,
    ROUND(STDDEV(agent_hired_on_rank), 2) AS stddev_agent_hired_on_rank
FROM ranked
GROUP BY GROUPING SETS ((channel, agent_tier), (channel))
