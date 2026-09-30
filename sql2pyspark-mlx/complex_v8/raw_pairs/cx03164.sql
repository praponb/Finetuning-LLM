WITH valid AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE HOUR(opened_at) BETWEEN 7 AND 9
),
enriched AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.tags,
        su.hired_on AS agent_hired_on
    FROM valid s
    LEFT JOIN support_agents su ON su.id = s.agent_id AND su.hired_on BETWEEN DATE '2024-04-14' AND DATE '2025-11-09'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY channel)) AS distinct_agent_id_in_channel,
        ROUND(requester_id - AVG(requester_id) OVER w_channel, 2) AS requester_id_vs_channel_avg,
        ROUND(PERCENT_RANK() OVER (ORDER BY closed_at DESC, id), 3) AS closed_at_percent_rank,
        ROUND(AVG(requester_id) OVER (ORDER BY closed_at DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS requester_id_avg_3_prior,
        RANK() OVER w_agent_id_by_agent_hired_on_t AS agent_hired_on_rank,
        COUNT(*) OVER (PARTITION BY csat) AS rows_in_csat,
        ROUND(CUME_DIST() OVER (ORDER BY closed_at DESC), 3) AS closed_at_cume_dist,
        MIN(requester_id) OVER (PARTITION BY channel) AS channel_min_requester_id,
        MAX(csat) OVER (PARTITION BY csat) AS csat_max_csat,
        ROUND(csat - AVG(csat) OVER w_channel, 2) AS csat_vs_channel_avg,
        LEAD(csat) OVER (PARTITION BY agent_id, csat ORDER BY csat, id) AS next_csat,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER w_csat, 0), 4) AS requester_id_share_of_csat
    FROM enriched
    WINDOW w_channel AS (PARTITION BY channel), w_agent_id_by_agent_hired_on_t AS (PARTITION BY agent_id ORDER BY agent_hired_on ASC), w_csat AS (PARTITION BY csat)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY closed_at_cume_dist) AS closed_at_cume_dist_dense_rank,
        ROUND(closed_at_percent_rank / NULLIF(SUM(closed_at_percent_rank) OVER w_channel_next_csat, 0), 4) AS closed_at_percent_rank_share_of_channel_next_csa,
        ROUND(AVG(next_csat) OVER (ORDER BY closed_at_cume_dist, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS next_csat_avg_3_prior,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY channel, next_csat), 0), 4) AS csat_share_of_channel_next_csat
    FROM ranked
    WINDOW w_channel_next_csat AS (PARTITION BY channel, next_csat)
)
SELECT
    id,
    requester_id,
    agent_id,
    channel,
    tags,
    agent_hired_on,
    distinct_agent_id_in_channel,
    closed_at_percent_rank,
    requester_id_avg_3_prior,
    agent_hired_on_rank,
    rows_in_csat,
    closed_at_cume_dist,
    csat_vs_channel_avg,
    next_csat,
    requester_id_share_of_csat,
    closed_at_cume_dist_dense_rank,
    closed_at_percent_rank_share_of_channel_next_csa,
    next_csat_avg_3_prior,
    csat_share_of_channel_next_csat
FROM sequenced
