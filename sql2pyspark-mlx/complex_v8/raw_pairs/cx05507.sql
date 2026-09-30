WITH recent_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        channel,
        tags
    FROM tickets
    WHERE channel = 'email'
),
joined AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.channel,
        s.tags,
        su.agent_name,
        su.hired_on AS agent_hired_on
    FROM recent_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(agent_hired_on), YEAR(opened_at) ORDER BY requester_id DESC), 3) AS requester_id_percent_rank,
        NTILE(4) OVER w_agent_hired_on_year_opened_a_2 AS requester_id_quartile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY agent_id ORDER BY agent_hired_on), 3) AS agent_hired_on_percent_rank,
        MAX(requester_id) OVER w_agent_id AS agent_id_max_requester_id,
        SIZE(COLLECT_SET(channel) OVER (PARTITION BY agent_id)) AS distinct_channel_in_agent_id,
        SUM(requester_id) OVER (PARTITION BY agent_id ORDER BY agent_hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_requester_id,
        RANK() OVER (PARTITION BY agent_id ORDER BY agent_hired_on) AS agent_hired_on_rank,
        SUM(requester_id) OVER (PARTITION BY channel, agent_id ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_requester_id_2,
        LEAD(requester_id, 3) OVER (PARTITION BY agent_id ORDER BY agent_hired_on, id) AS next3_requester_id,
        COUNT(*) OVER (PARTITION BY agent_id) AS rows_in_agent_id,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER w_agent_id, 0), 4) AS requester_id_share_of_agent_id,
        SUM(requester_id) OVER (PARTITION BY agent_id) AS agent_id_sum_requester_id,
        FIRST_VALUE(agent_hired_on) IGNORE NULLS OVER (PARTITION BY YEAR(agent_hired_on), YEAR(opened_at) ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_agent_hired_on
    FROM joined
    WINDOW w_agent_hired_on_year_opened_a_2 AS (PARTITION BY YEAR(agent_hired_on), YEAR(opened_at) ORDER BY requester_id DESC, id), w_agent_id AS (PARTITION BY agent_id)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(next3_requester_id) OVER w_channel_agent_id, 2) AS channel_agent_id_avg_next3_requester_id,
        DENSE_RANK() OVER (ORDER BY agent_hired_on DESC NULLS FIRST, id) AS agent_hired_on_dense_rank,
        NTILE(4) OVER (PARTITION BY MONTH(agent_hired_on), channel ORDER BY next3_requester_id NULLS LAST, id) AS next3_requester_id_quartile,
        rows_in_agent_id - LAG(rows_in_agent_id) OVER w_all_by_next3_requester_id AS rows_in_agent_id_change,
        RANK() OVER w_agent_hired_on_month_channel AS next3_requester_id_rank
    FROM ranked
    WINDOW w_channel_agent_id AS (PARTITION BY channel, agent_id), w_all_by_next3_requester_id AS (ORDER BY next3_requester_id DESC NULLS FIRST, id), w_agent_hired_on_month_channel AS (PARTITION BY MONTH(agent_hired_on), channel ORDER BY next3_requester_id NULLS LAST, id)
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    opened_at,
    channel,
    tags,
    agent_name,
    agent_hired_on,
    requester_id_quartile,
    agent_hired_on_percent_rank,
    distinct_channel_in_agent_id,
    running_requester_id,
    agent_hired_on_rank,
    next3_requester_id,
    rows_in_agent_id,
    requester_id_share_of_agent_id,
    agent_id_sum_requester_id,
    channel_agent_id_avg_next3_requester_id,
    agent_hired_on_dense_rank,
    next3_requester_id_quartile,
    rows_in_agent_id_change,
    next3_requester_id_rank,
    COALESCE(running_requester_id, 0) AS running_requester_id_filled
FROM with_windows
ORDER BY agent_hired_on_rank
