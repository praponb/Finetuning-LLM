WITH valid AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        tags,
        CASE WHEN channel IN ('chat', 'web') THEN 'group_a' WHEN channel = 'phone' THEN 'group_b' ELSE 'other' END AS channel_group
    FROM tickets
    WHERE requester_id < 19
),
combined_details_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team,
        su.tier AS agent_tier
    FROM valid s
    JOIN support_agents su ON su.id = s.agent_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER w_agent_id_by_closed_at AS closed_at_rank,
        FIRST_VALUE(agent_hired_on) OVER (PARTITION BY agent_tier, agent_id ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_agent_hired_on,
        DENSE_RANK() OVER (PARTITION BY agent_id, agent_team ORDER BY agent_hired_on DESC) AS agent_hired_on_dense_rank,
        DATEDIFF(agent_hired_on, LAG(agent_hired_on) OVER (ORDER BY closed_at DESC, id)) AS days_since_prev_agent_hired_on,
        LAST_VALUE(closed_at) IGNORE NULLS OVER (PARTITION BY agent_tier, agent_id ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_closed_at,
        LAST_VALUE(agent_hired_on) OVER (ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_agent_hired_on,
        NTILE(4) OVER w_agent_id_agent_team_by_agent_2 AS agent_hired_on_quartile,
        ROW_NUMBER() OVER (PARTITION BY agent_tier, agent_id ORDER BY closed_at DESC, id) AS closed_at_seq,
        NTH_VALUE(agent_hired_on, 2) OVER (PARTITION BY agent_id ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_agent_hired_on,
        ROUND(PERCENT_RANK() OVER (PARTITION BY agent_id, agent_team ORDER BY agent_hired_on DESC, id), 3) AS agent_hired_on_percent_rank,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY agent_id ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        LAG(closed_at, 3) OVER (PARTITION BY agent_team, agent_tier ORDER BY agent_hired_on DESC, id) AS prev3_closed_at
    FROM combined_details_agent
    WINDOW w_agent_id_by_closed_at AS (PARTITION BY agent_id ORDER BY closed_at, id), w_agent_id_agent_team_by_agent_2 AS (PARTITION BY agent_id, agent_team ORDER BY agent_hired_on DESC, id)
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT_WS('-', agent_tier, agent_team) AS agent_tier_agent_team_label,
        DATE_FORMAT(closed_at, 'MMM yyyy') AS closed_at_label
    FROM metrics
    WHERE second_closed_at >= TIMESTAMP '2025-02-02 03:00:00'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(agent_hired_on_percent_rank) OVER (ORDER BY second_agent_hired_on DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS agent_hired_on_percent_rank_max_1_prior,
        LEAD(closed_at) OVER (ORDER BY second_agent_hired_on DESC, id) AS next_closed_at,
        COUNT(*) OVER (ORDER BY second_agent_hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        MAX(closed_at_seq) OVER (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on)) AS agent_hired_on_month_max_closed_at_seq,
        ROW_NUMBER() OVER (ORDER BY agent_hired_on_percent_rank DESC, id) AS seq_no,
        ROUND(agent_hired_on_quartile - AVG(agent_hired_on_quartile) OVER w_agent_hired_on_month, 2) AS agent_hired_on_quartile_vs_agent_hired_on_month_,
        LAST_VALUE(second_agent_hired_on) IGNORE NULLS OVER (ORDER BY agent_hired_on_percent_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_second_agent_hired_on
    FROM labeled
    WINDOW w_agent_hired_on_month AS (PARTITION BY DATE_TRUNC('MONTH', agent_hired_on))
)
-- Final projection
SELECT
    id,
    agent_id,
    closed_at,
    agent_team,
    agent_tier,
    agent_hired_on_dense_rank,
    last_known_closed_at,
    last_agent_hired_on,
    second_agent_hired_on,
    agent_hired_on_percent_rank,
    second_closed_at,
    prev3_closed_at,
    next_closed_at,
    running_count,
    seq_no,
    agent_hired_on_quartile_vs_agent_hired_on_month_,
    last_known_second_agent_hired_on
FROM windowed
