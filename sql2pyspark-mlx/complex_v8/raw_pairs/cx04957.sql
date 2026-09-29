WITH base_tickets AS (
    -- Scope the tickets rows
    SELECT *
    FROM tickets
    WHERE channel NOT IN ('web', 'email')
),
with_details AS (
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
        su.tier AS agent_tier,
        su.hired_on AS agent_hired_on
    FROM base_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        opened_at,
        closed_at,
        channel,
        tags,
        agent_tier,
        LAG(requester_id) OVER w_agent_id_csat_by_csat AS prev_requester_id,
        MIN(requester_id) OVER (PARTITION BY priority, agent_id) AS priority_agent_id_min_requester_id,
        FIRST_VALUE(requester_id) IGNORE NULLS OVER (PARTITION BY priority ORDER BY opened_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_requester_id,
        COUNT(*) OVER (PARTITION BY priority, agent_id) AS rows_in_priority_agent_id,
        SUM(csat) OVER (PARTITION BY agent_id, csat ORDER BY csat, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS csat_sum_6_prior,
        ROUND(AVG(csat) OVER (PARTITION BY priority ORDER BY opened_at NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS csat_avg_1_prior,
        ROUND(requester_id - AVG(requester_id) OVER (PARTITION BY agent_tier, channel), 2) AS requester_id_vs_agent_tier_channel_avg,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY agent_tier, channel), 0), 4) AS requester_id_share_of_agent_tier_channel
    FROM with_details
    WINDOW w_agent_id_csat_by_csat AS (PARTITION BY agent_id, csat ORDER BY csat, id)
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 60, 1) AS minutes_opened_at_to_closed_at
    FROM ranked
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        opened_at,
        closed_at,
        channel,
        tags,
        agent_tier,
        prev_requester_id,
        priority_agent_id_min_requester_id,
        rows_in_priority_agent_id,
        csat_sum_6_prior,
        requester_id_vs_agent_tier_channel_avg,
        requester_id_share_of_agent_tier_channel,
        ROUND(prev_requester_id - AVG(prev_requester_id) OVER (PARTITION BY channel, agent_tier), 2) AS prev_requester_id_vs_channel_agent_tier_avg,
        ROUND(AVG(prev_requester_id) OVER (ORDER BY minutes_opened_at_to_closed_at ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS prev_requester_id_avg_3,
        ROUND(AVG(rows_in_priority_agent_id) OVER (PARTITION BY channel, agent_tier), 2) AS channel_agent_tier_avg_rows_in_priority_agent_id,
        ROUND(rows_in_priority_agent_id - AVG(rows_in_priority_agent_id) OVER w_channel_agent_tier, 2) AS rows_in_priority_agent_id_vs_channel_agent_tier_,
        LAG(prev_requester_id, 3) OVER w_all_by_closed_at AS prev3_prev_requester_id,
        DENSE_RANK() OVER (ORDER BY closed_at DESC, id) AS closed_at_dense_rank
    FROM annotated
    WINDOW w_channel_agent_tier AS (PARTITION BY channel, agent_tier), w_all_by_closed_at AS (ORDER BY closed_at DESC, id)
),
rollup_base AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(opened_at, 'yyyy-MM') AS opened_at_ym,
        COUNT(*) AS cnt,
        ROUND(STDDEV(prev_requester_id_avg_3), 2) AS stddev_prev_requester_id_avg_3,
        MIN(opened_at) AS first_opened_at,
        MIN(channel_agent_tier_avg_rows_in_priority_agent_id) AS min_channel_agent_tier_avg_rows_in_priority_agen
    FROM metrics
    GROUP BY DATE_FORMAT(opened_at, 'yyyy-MM')
)
SELECT DISTINCT cnt
FROM rollup_base
WHERE TO_DATE(first_opened_at) < DATE '2025-02-18'
ORDER BY cnt NULLS LAST
