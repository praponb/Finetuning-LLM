WITH recent AS (
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
        tags,
        ROUND(csat / NULLIF(requester_id, 0), 3) AS csat_per_requester_id,
        INITCAP(TRIM(channel)) AS channel_clean
    FROM tickets
    WHERE priority <> 'P1'
),
joined AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.tags,
        s.channel_clean,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier,
        su.agent_name
    FROM recent s
    JOIN support_agents su ON su.id = s.agent_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER w_priority_agent_tier_by_close AS closed_at_rank,
        DENSE_RANK() OVER (PARTITION BY agent_tier, agent_id ORDER BY csat DESC) AS csat_dense_rank,
        MAX(requester_id) OVER w_agent_id AS agent_id_max_requester_id,
        COUNT(*) OVER w_channel_csat AS rows_in_channel_csat,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY channel, csat), 0), 4) AS requester_id_share_of_channel_csat,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(LAG(opened_at) OVER (PARTITION BY channel, agent_id ORDER BY agent_hired_on NULLS LAST, id))) / 60, 1) AS minutes_since_prev_opened_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY channel, agent_id ORDER BY agent_hired_on NULLS LAST), 3) AS agent_hired_on_percent_rank
    FROM joined
    WINDOW w_priority_agent_tier_by_close AS (PARTITION BY priority, agent_tier ORDER BY closed_at, id), w_agent_id AS (PARTITION BY agent_id), w_channel_csat AS (PARTITION BY channel, csat)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_agent_id_agent_tier_by_rows, 3) AS rows_in_channel_csat_percent_rank,
        SUM(agent_hired_on_percent_rank) OVER (PARTITION BY agent_tier, DATE_TRUNC('MONTH', closed_at) ORDER BY requester_id_share_of_channel_csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_agent_hired_on_percent_rank,
        LAG(csat) OVER (PARTITION BY csat_dense_rank, YEAR(opened_at) ORDER BY requester_id_share_of_channel_csat DESC, id) AS prev_csat,
        NTH_VALUE(csat, 2) OVER (PARTITION BY priority, csat ORDER BY requester_id_share_of_channel_csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat,
        MAX(csat_dense_rank) OVER (PARTITION BY priority) AS priority_max_csat_dense_rank,
        ROUND(agent_id_max_requester_id - AVG(agent_id_max_requester_id) OVER (PARTITION BY priority), 2) AS agent_id_max_requester_id_vs_priority_avg,
        FIRST_VALUE(requester_id) OVER (PARTITION BY agent_id, agent_tier ORDER BY rows_in_channel_csat ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_requester_id,
        MIN(closed_at_rank) OVER (PARTITION BY priority) AS priority_min_closed_at_rank
    FROM with_windows
    WINDOW w_agent_id_agent_tier_by_rows AS (PARTITION BY agent_id, agent_tier ORDER BY rows_in_channel_csat ASC)
)
-- Final projection
SELECT
    id,
    requester_id,
    opened_at,
    closed_at,
    priority,
    csat,
    tags,
    channel_clean,
    agent_name,
    closed_at_rank,
    csat_dense_rank,
    agent_id_max_requester_id,
    minutes_since_prev_opened_at,
    agent_hired_on_percent_rank,
    rows_in_channel_csat_percent_rank,
    running_agent_hired_on_percent_rank,
    prev_csat,
    second_csat,
    priority_max_csat_dense_rank,
    agent_id_max_requester_id_vs_priority_avg,
    first_requester_id,
    ARRAY_JOIN(ARRAY_SORT(tags), ',') AS tags_text
FROM with_trends
WHERE requester_id <= 21
