WITH base AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        csat,
        tags
    FROM tickets
),
with_parent_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.csat,
        s.tags,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM base s
    LEFT JOIN support_agents su ON su.id = s.agent_id AND su.tier IN ('T3', 'T1')
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(csat) OVER (ORDER BY requester_id, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS csat_max_1_prior,
        LEAD(requester_id, 2) OVER (PARTITION BY agent_id, agent_team ORDER BY requester_id, id) AS next2_requester_id,
        LAG(requester_id, 3, 0) OVER w_csat_agent_team_by_requester AS prev3_requester_id,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY agent_tier)) AS distinct_agent_id_in_agent_tier,
        MAX(requester_id) OVER (PARTITION BY TO_DATE(closed_at), agent_team) AS closed_at_to_date_agent_team_max_requester_id,
        DENSE_RANK() OVER (PARTITION BY agent_id, agent_team ORDER BY requester_id, id) AS requester_id_dense_rank,
        ROUND(requester_id / NULLIF(SUM(requester_id) OVER (PARTITION BY csat), 0), 4) AS requester_id_share_of_csat,
        ROUND(AVG(requester_id) OVER (PARTITION BY csat, agent_team ORDER BY requester_id, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS requester_id_avg_4
    FROM with_parent_agent
    WINDOW w_csat_agent_team_by_requester AS (PARTITION BY csat, agent_team ORDER BY requester_id, id)
),
first_rows AS (
    -- Top 5 per group
    SELECT *
    FROM with_trends
    WHERE requester_id_dense_rank <= 5
      AND closed_at_to_date_agent_team_max_requester_id BETWEEN 63 AND 188
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER w_all_by_closed_at_to_date_age AS seq_no,
        ROUND(AVG(next2_requester_id) OVER (ORDER BY closed_at_to_date_agent_team_max_requester_id, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS next2_requester_id_avg_6_prior,
        next2_requester_id - LAG(next2_requester_id) OVER (ORDER BY closed_at_to_date_agent_team_max_requester_id, id) AS next2_requester_id_change,
        ROUND(next2_requester_id - AVG(next2_requester_id) OVER (PARTITION BY csat, requester_id_dense_rank), 2) AS next2_requester_id_vs_csat_requester_id_dense_ra,
        requester_id_avg_4 - LAG(requester_id_avg_4) OVER (PARTITION BY requester_id_dense_rank, distinct_agent_id_in_agent_tier ORDER BY next2_requester_id NULLS LAST, id) AS requester_id_avg_4_change
    FROM first_rows
    WINDOW w_all_by_closed_at_to_date_age AS (ORDER BY closed_at_to_date_agent_team_max_requester_id, id)
)
SELECT
    id,
    requester_id,
    agent_id,
    csat,
    tags,
    agent_tier,
    agent_team,
    csat_max_1_prior,
    next2_requester_id,
    prev3_requester_id,
    distinct_agent_id_in_agent_tier,
    closed_at_to_date_agent_team_max_requester_id,
    requester_id_dense_rank,
    requester_id_share_of_csat,
    requester_id_avg_4,
    seq_no,
    next2_requester_id_avg_6_prior,
    next2_requester_id_change,
    next2_requester_id_vs_csat_requester_id_dense_ra,
    requester_id_avg_4_change,
    LOWER(TRIM(agent_tier)) AS agent_tier_clean,
    SUBSTRING(TRIM(agent_tier), 1, 3) AS agent_tier_prefix
FROM with_windows
WHERE agent_team = 'Tech'
ORDER BY agent_tier_prefix DESC, next2_requester_id_change, id
LIMIT 20
