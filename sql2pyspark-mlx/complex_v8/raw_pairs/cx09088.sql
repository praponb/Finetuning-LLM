WITH base_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        DATE_TRUNC('DAY', opened_at) AS opened_at_day,
        LEAST(csat, requester_id) AS least_csat_requester_id
    FROM tickets
    WHERE channel = 'email'
      AND SIZE(tags) >= 1
),
combined_details AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.channel,
        s.tags,
        s.opened_at_day,
        s.least_csat_requester_id,
        su.hired_on AS agent_hired_on,
        su.agent_name,
        su.tier AS agent_tier
    FROM base_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(requester_id) OVER (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on), agent_id ORDER BY least_csat_requester_id ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS requester_id_avg_3_prior,
        COUNT(*) OVER (PARTITION BY priority, agent_id) AS rows_in_priority_agent_id,
        COUNT(*) OVER w_channel AS rows_in_channel,
        LAG(agent_hired_on) OVER w_agent_id_agent_tier_by_close AS prev_agent_hired_on,
        ROW_NUMBER() OVER (ORDER BY least_csat_requester_id ASC, id) AS least_csat_requester_id_seq,
        MAX(least_csat_requester_id) OVER (ORDER BY least_csat_requester_id ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS least_csat_requester_id_max_6_prior,
        ROUND(AVG(least_csat_requester_id) OVER (PARTITION BY agent_id, agent_tier ORDER BY closed_at DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS least_csat_requester_id_avg_7
    FROM combined_details
    WINDOW w_channel AS (PARTITION BY channel), w_agent_id_agent_tier_by_close AS (PARTITION BY agent_id, agent_tier ORDER BY closed_at DESC NULLS FIRST, id), w_priority_agent_id AS (PARTITION BY priority, agent_id)
),
top_rows AS (
    -- Top 3 per group
    SELECT *
    FROM ranked
    WHERE least_csat_requester_id_seq <= 3
      AND channel IN ('phone', 'web', 'chat')
)
SELECT
    id,
    requester_id,
    agent_id,
    closed_at,
    priority,
    channel,
    tags,
    opened_at_day,
    agent_hired_on,
    agent_name,
    agent_tier,
    requester_id_avg_3_prior,
    rows_in_priority_agent_id,
    prev_agent_hired_on,
    least_csat_requester_id_seq,
    least_csat_requester_id_max_6_prior
FROM top_rows
ORDER BY agent_hired_on DESC NULLS FIRST, agent_tier
