WITH base_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        channel,
        csat,
        tags,
        INITCAP(TRIM(priority)) AS priority_clean,
        GREATEST(csat, requester_id) AS greatest_csat_requester_id
    FROM tickets
),
with_details AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.channel,
        s.csat,
        s.greatest_csat_requester_id,
        su.tier AS agent_tier
    FROM base_tickets s
    JOIN support_agents su ON su.id = s.agent_id AND su.hired_on IS NOT NULL
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        agent_id,
        opened_at,
        channel,
        greatest_csat_requester_id,
        agent_tier,
        LEAD(csat) OVER (PARTITION BY channel, csat ORDER BY greatest_csat_requester_id, id) AS next_csat,
        NTH_VALUE(requester_id, 2) OVER (PARTITION BY channel, agent_tier ORDER BY csat DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id,
        MAX(csat) OVER (PARTITION BY channel) AS channel_max_csat,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY channel), 0), 4) AS csat_share_of_channel,
        ROUND(greatest_csat_requester_id / NULLIF(SUM(greatest_csat_requester_id) OVER (PARTITION BY agent_id, MONTH(opened_at)), 0), 4) AS greatest_csat_requester_id_share_of_agent_id_ope,
        SUM(greatest_csat_requester_id) OVER (PARTITION BY channel, csat ORDER BY greatest_csat_requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_greatest_csat_requester_id,
        ROUND(greatest_csat_requester_id - AVG(greatest_csat_requester_id) OVER (PARTITION BY agent_id, MONTH(opened_at)), 2) AS greatest_csat_requester_id_vs_agent_id_opened_at,
        SUM(greatest_csat_requester_id) OVER (PARTITION BY agent_tier ORDER BY greatest_csat_requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_greatest_csat_requester_id_2,
        LAG(csat) OVER (ORDER BY opened_at, id) AS prev_csat,
        MAX(requester_id) OVER (PARTITION BY agent_tier ORDER BY greatest_csat_requester_id, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS requester_id_max_1_prior,
        COUNT(*) OVER (PARTITION BY channel) AS rows_in_channel
    FROM with_details
),
summary AS (
    -- Summarise per group
    SELECT
        next_csat,
        prev_csat,
        SUM(CASE WHEN agent_tier <> 'T2' THEN 1 ELSE 0 END) AS n_agent_tier_t2,
        SUM(CASE WHEN channel = 'web' THEN next_csat ELSE 0 END) AS next_csat_channel_web,
        ROUND(AVG(next_csat), 2) AS avg_next_csat,
        SUM(CASE WHEN channel IS NOT NULL THEN 1 ELSE 0 END) AS n_channel_is_not_null
    FROM metrics
    GROUP BY 1, 2
)
-- Final projection
SELECT
    next_csat,
    prev_csat,
    n_agent_tier_t2,
    next_csat_channel_web,
    n_channel_is_not_null,
    ROUND(n_agent_tier_t2 / NULLIF(next_csat, 0), 3) AS n_agent_tier_t2_per_next_csat
FROM summary
WHERE avg_next_csat < 3.8
ORDER BY n_channel_is_not_null NULLS LAST, next_csat DESC
