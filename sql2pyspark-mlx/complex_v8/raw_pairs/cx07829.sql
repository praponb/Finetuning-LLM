WITH prepared AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        ROUND(csat / NULLIF(requester_id, 0), 3) AS csat_per_requester_id,
        ARRAY_CONTAINS(tags, 'refund') AS has_refund
    FROM tickets
    WHERE closed_at < TIMESTAMP '2025-02-10 04:00:00'
      AND agent_id >= 7
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.csat,
        s.csat_per_requester_id,
        s.has_refund,
        su.team AS agent_team,
        su.tier AS agent_tier,
        su.agent_name
    FROM prepared s
    JOIN support_agents su ON su.id = s.agent_id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(requester_id) OVER (PARTITION BY agent_id) AS agent_id_max_requester_id,
        ROW_NUMBER() OVER w_csat_has_refund_by_closed_at AS seq_no,
        SUM(csat_per_requester_id) OVER (PARTITION BY agent_tier, agent_id ORDER BY requester_id DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat_per_requester_id,
        ROUND(CUME_DIST() OVER (PARTITION BY agent_id, agent_tier ORDER BY csat ASC NULLS LAST), 3) AS csat_cume_dist,
        SUM(requester_id) OVER (PARTITION BY agent_id) AS agent_id_sum_requester_id,
        MAX(csat_per_requester_id) OVER (PARTITION BY has_refund, agent_tier) AS has_refund_agent_tier_max_csat_per_requester_id,
        MAX(csat) OVER (PARTITION BY agent_id) AS agent_id_max_csat,
        LAST_VALUE(csat) OVER (PARTITION BY agent_team, has_refund ORDER BY csat ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_csat,
        ROUND(PERCENT_RANK() OVER (PARTITION BY csat, has_refund ORDER BY closed_at), 3) AS closed_at_percent_rank,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY agent_team, has_refund ORDER BY csat ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        LEAD(csat_per_requester_id) OVER (PARTITION BY csat, has_refund ORDER BY closed_at, id) AS next_csat_per_requester_id,
        NTILE(10) OVER w_agent_id_agent_tier_by_csat_2 AS csat_decile,
        ROUND(csat_per_requester_id - AVG(csat_per_requester_id) OVER w_agent_id, 2) AS csat_per_requester_id_vs_agent_id_avg
    FROM enriched
    WINDOW w_csat_has_refund_by_closed_at AS (PARTITION BY csat, has_refund ORDER BY closed_at, id), w_agent_id_agent_tier_by_csat_2 AS (PARTITION BY agent_id, agent_tier ORDER BY csat ASC NULLS LAST, id), w_agent_id AS (PARTITION BY agent_id)
),
top_per_group AS (
    -- Top 3 per group
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        csat,
        csat_per_requester_id,
        has_refund,
        agent_team,
        agent_tier,
        agent_name,
        agent_id_max_requester_id,
        running_csat_per_requester_id,
        csat_cume_dist,
        agent_id_sum_requester_id,
        has_refund_agent_tier_max_csat_per_requester_id,
        agent_id_max_csat,
        last_csat,
        closed_at_percent_rank,
        second_closed_at,
        next_csat_per_requester_id,
        csat_decile,
        csat_per_requester_id_vs_agent_id_avg
    FROM with_windows
    WHERE seq_no <= 3
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        csat,
        csat_per_requester_id,
        has_refund,
        agent_team,
        agent_tier,
        agent_name,
        agent_id_max_requester_id,
        running_csat_per_requester_id,
        csat_cume_dist,
        agent_id_sum_requester_id,
        agent_id_max_csat,
        next_csat_per_requester_id,
        csat_per_requester_id_vs_agent_id_avg,
        SIZE(COLLECT_SET(agent_team) OVER w_csat) AS distinct_agent_team_in_csat,
        LAST_VALUE(csat_per_requester_id) OVER (PARTITION BY agent_team, last_csat ORDER BY next_csat_per_requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_csat_per_requester_id,
        NTILE(10) OVER (PARTITION BY csat, agent_team ORDER BY csat ASC NULLS LAST, id) AS csat_decile_2,
        LEAD(agent_id_sum_requester_id) OVER (PARTITION BY agent_team, last_csat ORDER BY next_csat_per_requester_id, id) AS next_agent_id_sum_requester_id,
        ROUND(AVG(csat_per_requester_id) OVER (PARTITION BY csat), 2) AS csat_avg_csat_per_requester_id,
        MIN(closed_at_percent_rank) OVER (PARTITION BY csat) AS csat_min_closed_at_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY csat, agent_team ORDER BY csat ASC NULLS LAST), 3) AS csat_cume_dist_2
    FROM top_per_group
    WINDOW w_csat AS (PARTITION BY csat)
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    closed_at,
    has_refund,
    agent_name,
    agent_id_max_requester_id,
    running_csat_per_requester_id,
    csat_cume_dist,
    agent_id_max_csat,
    next_csat_per_requester_id,
    csat_per_requester_id_vs_agent_id_avg,
    distinct_agent_team_in_csat,
    last_csat_per_requester_id,
    csat_decile_2,
    next_agent_id_sum_requester_id,
    csat_avg_csat_per_requester_id,
    csat_min_closed_at_percent_rank
FROM sequenced
WHERE next_csat_per_requester_id <= 7.5
ORDER BY next_agent_id_sum_requester_id DESC
