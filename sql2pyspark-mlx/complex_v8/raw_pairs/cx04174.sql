WITH recent AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        DATEDIFF(DATE '2025-06-30', opened_at) AS days_since_opened_at
    FROM tickets
    WHERE channel <> 'phone'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(closed_at) OVER (PARTITION BY priority ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_closed_at,
        LEAD(requester_id) OVER (ORDER BY days_since_opened_at DESC, id) AS next_requester_id,
        ROUND(csat - AVG(csat) OVER (PARTITION BY priority), 2) AS csat_vs_priority_avg,
        LEAD(days_since_opened_at) OVER (ORDER BY days_since_opened_at DESC, id) AS next_days_since_opened_at
    FROM recent
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        days_since_opened_at,
        next_requester_id,
        csat_vs_priority_avg,
        next_days_since_opened_at,
        LENGTH(TRIM(channel)) AS channel_len,
        CASE WHEN next_requester_id >= 16 THEN 'HIGH' WHEN next_requester_id >= 7 THEN 'MEDIUM' ELSE 'LOW' END AS next_requester_id_tier,
        ROUND((UNIX_TIMESTAMP(first_closed_at) - UNIX_TIMESTAMP(closed_at)) / 60, 1) AS minutes_closed_at_to_first_closed_at
    FROM metrics
),
joined_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.days_since_opened_at,
        s.next_requester_id,
        s.csat_vs_priority_avg,
        s.minutes_closed_at_to_first_closed_at,
        su.tier AS agent_tier
    FROM annotated s
    LEFT JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    agent_id,
    channel,
    GROUPING(agent_id) AS is_total_agent_id,
    MIN(closed_at) AS first_closed_at,
    ROUND(100 * COUNT(CASE WHEN channel = 'phone' THEN 1 END) / COUNT(*), 1) AS pct_channel_phone,
    PERCENTILE_APPROX(minutes_closed_at_to_first_closed_at, 0.5) AS median_minutes_closed_at_to_first_closed_at
FROM joined_agent
GROUP BY GROUPING SETS ((agent_id, channel), (agent_id), ())
