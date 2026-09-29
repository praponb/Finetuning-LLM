WITH filtered_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags
    FROM tickets
    WHERE agent_id >= 2
      AND channel = 'phone'
),
joined_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.channel,
        s.csat,
        s.tags,
        su.tier AS agent_tier
    FROM filtered_tickets s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.tier = 'T2'
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(requester_id) OVER (PARTITION BY agent_id, csat ORDER BY requester_id DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS requester_id_max_7,
        ROUND(AVG(requester_id) OVER (PARTITION BY agent_tier, csat ORDER BY requester_id, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS requester_id_avg_7,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY agent_tier)) AS distinct_agent_id_in_agent_tier,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY agent_id), 0), 4) AS requester_id_share_of_agent_id,
        MAX(requester_id) OVER (PARTITION BY agent_id) AS agent_id_max_requester_id,
        NTILE(3) OVER (PARTITION BY agent_tier, csat ORDER BY requester_id, id) AS requester_id_tercile,
        SIZE(COLLECT_SET(agent_tier) OVER w_agent_id) AS distinct_agent_tier_in_agent_id,
        LAST_VALUE(requester_id) OVER (PARTITION BY channel, agent_id ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_requester_id,
        MIN(csat) OVER (PARTITION BY agent_tier) AS agent_tier_min_csat,
        COUNT(*) OVER (PARTITION BY agent_id) AS rows_in_agent_id,
        DENSE_RANK() OVER w_csat_agent_tier_by_csat_ties AS csat_dense_rank,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(LAG(closed_at) OVER (PARTITION BY agent_id, csat ORDER BY requester_id DESC, id))) / 60, 1) AS minutes_since_prev_closed_at,
        FIRST_VALUE(csat) IGNORE NULLS OVER (PARTITION BY agent_id, csat ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_csat,
        MIN(requester_id) OVER w_agent_tier AS agent_tier_min_requester_id,
        LAST_VALUE(requester_id) OVER (PARTITION BY csat, agent_tier ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_requester_id_2
    FROM joined_agent
    WINDOW w_agent_id AS (PARTITION BY agent_id), w_csat_agent_tier_by_csat_ties AS (PARTITION BY csat, agent_tier ORDER BY csat), w_agent_tier AS (PARTITION BY agent_tier)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        requester_id,
        agent_id,
        channel,
        csat,
        agent_tier,
        requester_id_avg_7,
        distinct_agent_id_in_agent_tier,
        requester_id_share_of_agent_id,
        agent_id_max_requester_id,
        distinct_agent_tier_in_agent_id,
        rows_in_agent_id,
        csat_dense_rank,
        minutes_since_prev_closed_at,
        first_known_csat,
        agent_tier_min_requester_id,
        LAG(agent_id_max_requester_id) OVER (ORDER BY closed_at, id) AS prev_agent_id_max_requester_id,
        NTILE(10) OVER (PARTITION BY distinct_agent_id_in_agent_tier ORDER BY agent_tier_min_requester_id, id) AS agent_tier_min_requester_id_decile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY distinct_agent_id_in_agent_tier, agent_id ORDER BY minutes_since_prev_closed_at ASC), 3) AS minutes_since_prev_closed_at_percent_rank,
        ROUND(csat_dense_rank / NULLIF(SUM(csat_dense_rank) OVER (PARTITION BY agent_tier, first_known_csat), 0), 4) AS csat_dense_rank_share_of_agent_tier_first_known_,
        ROUND(PERCENT_RANK() OVER (PARTITION BY distinct_agent_id_in_agent_tier ORDER BY agent_tier_min_requester_id, id), 3) AS agent_tier_min_requester_id_percent_rank,
        minutes_since_prev_closed_at - LAG(minutes_since_prev_closed_at) OVER (PARTITION BY channel, csat_dense_rank ORDER BY requester_id_share_of_agent_id, id) AS minutes_since_prev_closed_at_change,
        ROW_NUMBER() OVER (ORDER BY closed_at, id) AS rn,
        MAX(last_requester_id_2) OVER (PARTITION BY agent_tier, first_known_csat) AS agent_tier_first_known_csat_max_last_requester_i
    FROM with_trends
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    requester_id_avg_7,
    distinct_agent_id_in_agent_tier,
    requester_id_share_of_agent_id,
    agent_id_max_requester_id,
    distinct_agent_tier_in_agent_id,
    rows_in_agent_id,
    csat_dense_rank,
    minutes_since_prev_closed_at,
    first_known_csat,
    agent_tier_min_requester_id,
    prev_agent_id_max_requester_id,
    agent_tier_min_requester_id_decile,
    minutes_since_prev_closed_at_percent_rank,
    csat_dense_rank_share_of_agent_tier_first_known_,
    agent_tier_min_requester_id_percent_rank,
    minutes_since_prev_closed_at_change,
    rn,
    CONCAT(channel, '#', CAST(csat_dense_rank AS STRING)) AS channel_ref,
    CONCAT(agent_tier, '#', CAST(csat AS STRING)) AS agent_tier_ref
FROM sequenced
