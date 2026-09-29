WITH base_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        tags,
        ROUND(csat / NULLIF(requester_id, 0), 3) AS csat_per_requester_id,
        CASE WHEN channel IN ('email', 'phone') THEN 'group_a' WHEN channel = 'chat' THEN 'group_b' ELSE 'other' END AS channel_group
    FROM tickets
    WHERE (channel = 'web' OR priority IS NOT NULL)
),
enriched_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.tags,
        s.channel_group,
        su.tier AS agent_tier,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team
    FROM base_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(agent_hired_on) IGNORE NULLS OVER (ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_agent_hired_on,
        LEAD(opened_at) OVER w_channel_group_by_requester_i AS next_opened_at,
        SIZE(COLLECT_SET(agent_team) OVER w_agent_tier) AS distinct_agent_team_in_agent_tier,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY agent_tier), 0), 4) AS requester_id_share_of_agent_tier,
        DENSE_RANK() OVER (PARTITION BY YEAR(opened_at), DATE_TRUNC('MONTH', agent_hired_on) ORDER BY agent_hired_on) AS agent_hired_on_dense_rank,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(LAG(opened_at) OVER w_opened_at_year_agent_hired_o_2)) / 60, 1) AS minutes_since_prev_opened_at,
        DATEDIFF(agent_hired_on, LAG(agent_hired_on) OVER (PARTITION BY channel_group ORDER BY requester_id DESC, id)) AS days_since_prev_agent_hired_on,
        NTH_VALUE(agent_hired_on, 2) OVER (PARTITION BY channel_group ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_agent_hired_on,
        ROUND(AVG(requester_id) OVER (PARTITION BY channel_group ORDER BY requester_id DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS requester_id_avg_6_prior,
        SIZE(COLLECT_SET(agent_id) OVER w_agent_tier) AS distinct_agent_id_in_agent_tier,
        ROUND(AVG(requester_id) OVER (PARTITION BY agent_tier), 2) AS agent_tier_avg_requester_id,
        LAG(opened_at) OVER w_all_by_requester_id AS prev_opened_at,
        ROUND(PERCENT_RANK() OVER w_all_by_requester_id_ties, 3) AS requester_id_percent_rank
    FROM enriched_agent
    WINDOW w_channel_group_by_requester_i AS (PARTITION BY channel_group ORDER BY requester_id DESC, id), w_agent_tier AS (PARTITION BY agent_tier), w_opened_at_year_agent_hired_o_2 AS (PARTITION BY YEAR(opened_at), DATE_TRUNC('MONTH', agent_hired_on) ORDER BY agent_hired_on, id), w_opened_at_year_agent_hired_o AS (PARTITION BY YEAR(opened_at), DATE_TRUNC('MONTH', agent_hired_on) ORDER BY agent_hired_on), w_all_by_requester_id AS (ORDER BY requester_id DESC, id), w_all_by_requester_id_ties AS (ORDER BY requester_id DESC)
),
leaders AS (
    -- First row per group
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        tags,
        channel_group,
        agent_tier,
        agent_hired_on,
        agent_team,
        last_known_agent_hired_on,
        next_opened_at,
        distinct_agent_team_in_agent_tier,
        requester_id_share_of_agent_tier,
        minutes_since_prev_opened_at,
        days_since_prev_agent_hired_on,
        second_agent_hired_on,
        requester_id_avg_6_prior,
        distinct_agent_id_in_agent_tier,
        agent_tier_avg_requester_id,
        prev_opened_at,
        requester_id_percent_rank
    FROM windowed
    WHERE agent_hired_on_dense_rank = 1
)
SELECT
    id,
    agent_id,
    opened_at,
    tags,
    channel_group,
    agent_tier,
    agent_hired_on,
    agent_team,
    next_opened_at,
    requester_id_share_of_agent_tier,
    minutes_since_prev_opened_at,
    days_since_prev_agent_hired_on,
    requester_id_avg_6_prior,
    distinct_agent_id_in_agent_tier,
    agent_tier_avg_requester_id,
    prev_opened_at,
    requester_id_percent_rank
FROM leaders
