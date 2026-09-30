WITH scoped_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        csat,
        INITCAP(TRIM(priority)) AS priority_clean
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'login')
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.csat,
        su.team AS agent_team,
        su.tier AS agent_tier
    FROM scoped_tickets s
    JOIN support_agents su ON su.id = s.agent_id AND su.tier NOT IN ('T2', 'T1')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        requester_id,
        csat,
        agent_team,
        agent_tier,
        ROUND(csat - AVG(csat) OVER (PARTITION BY agent_id, csat), 2) AS csat_vs_agent_id_csat_avg,
        SIZE(COLLECT_SET(agent_id) OVER w_agent_tier) AS distinct_agent_id_in_agent_tier,
        SUM(requester_id) OVER (PARTITION BY agent_id, agent_team) AS agent_id_agent_team_sum_requester_id,
        RANK() OVER (PARTITION BY agent_team ORDER BY requester_id ASC) AS requester_id_rank,
        ROUND(AVG(csat) OVER (ORDER BY csat, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS csat_avg_4,
        RANK() OVER (ORDER BY csat, id) AS csat_rank,
        MAX(csat) OVER (PARTITION BY agent_team ORDER BY requester_id ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS csat_max_2,
        MAX(csat) OVER (PARTITION BY agent_tier) AS agent_tier_max_csat,
        SUM(csat) OVER (PARTITION BY agent_team ORDER BY requester_id ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat,
        ROUND(csat - AVG(csat) OVER w_agent_id_agent_team, 2) AS csat_vs_agent_id_agent_team_avg,
        NTH_VALUE(requester_id, 2) OVER (PARTITION BY agent_tier ORDER BY requester_id ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id,
        COUNT(*) OVER (PARTITION BY agent_id, agent_team) AS rows_in_agent_id_agent_team,
        ROUND(AVG(requester_id) OVER (ORDER BY csat, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS requester_id_avg_2_prior,
        ROUND(requester_id - AVG(requester_id) OVER w_agent_id_agent_team, 2) AS requester_id_vs_agent_id_agent_team_avg,
        ROUND(requester_id - AVG(requester_id) OVER w_agent_tier, 2) AS requester_id_vs_agent_tier_avg,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY agent_id, csat), 0), 4) AS requester_id_share_of_agent_id_csat
    FROM with_parent
    WINDOW w_agent_tier AS (PARTITION BY agent_tier), w_agent_id_agent_team AS (PARTITION BY agent_id, agent_team)
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        GREATEST(requester_id, csat_rank) AS greatest_requester_id_csat_rank,
        UPPER(TRIM(agent_tier)) AS agent_tier_clean,
        COALESCE(csat_avg_4, 0.0) AS csat_avg_4_filled
    FROM metrics
    WHERE requester_id_vs_agent_tier_avg > -5.0
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_distinct_agent_id_in_agent_t, 3) AS running_csat_percent_rank,
        ROUND(csat_avg_4 - AVG(csat_avg_4) OVER (PARTITION BY requester_id_rank), 2) AS csat_avg_4_vs_requester_id_rank_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY distinct_agent_id_in_agent_tier ORDER BY running_csat ASC NULLS LAST, id), 3) AS running_csat_cume_dist,
        ROUND(PERCENT_RANK() OVER w_requester_id_rank_csat_rank, 3) AS csat_vs_agent_id_agent_team_avg_percent_rank,
        ROUND(second_requester_id - AVG(second_requester_id) OVER (PARTITION BY requester_id_rank), 2) AS second_requester_id_vs_requester_id_rank_avg
    FROM tagged
    WINDOW w_distinct_agent_id_in_agent_t AS (PARTITION BY distinct_agent_id_in_agent_tier ORDER BY running_csat ASC NULLS LAST), w_requester_id_rank_csat_rank AS (PARTITION BY requester_id_rank, csat_rank ORDER BY csat_vs_agent_id_agent_team_avg DESC)
),
rollup_base AS (
    -- Summarise per group
    SELECT
        distinct_agent_id_in_agent_tier,
        MAX(requester_id_rank) AS max_requester_id_rank,
        SUM(second_requester_id_vs_requester_id_rank_avg) AS total_second_requester_id_vs_requester_id_rank_a,
        COUNT(*) AS cnt,
        MIN(second_requester_id_vs_requester_id_rank_avg) AS min_second_requester_id_vs_requester_id_rank_avg
    FROM with_trends
    GROUP BY distinct_agent_id_in_agent_tier
)
SELECT
    distinct_agent_id_in_agent_tier,
    total_second_requester_id_vs_requester_id_rank_a,
    cnt,
    min_second_requester_id_vs_requester_id_rank_avg,
    ROUND(max_requester_id_rank / NULLIF(cnt, 0), 3) AS max_requester_id_rank_per_cnt
FROM rollup_base
WHERE total_second_requester_id_vs_requester_id_rank_a >= 45.0
ORDER BY max_requester_id_rank_per_cnt DESC, cnt DESC
