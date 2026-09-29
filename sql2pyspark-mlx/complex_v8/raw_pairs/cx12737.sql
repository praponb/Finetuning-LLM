WITH prepared_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        channel,
        csat
    FROM tickets
    WHERE csat > 1
),
with_parent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.channel,
        s.csat,
        su.team AS agent_team,
        su.tier AS agent_tier,
        su.agent_name
    FROM prepared_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(channel) OVER (PARTITION BY agent_team, agent_tier)) AS distinct_channel_in_agent_team_agent_tier,
        MIN(csat) OVER (PARTITION BY agent_team, agent_tier) AS agent_team_agent_tier_min_csat,
        LEAD(csat) OVER (ORDER BY requester_id DESC, id) AS next_csat,
        SIZE(COLLECT_SET(channel) OVER (PARTITION BY agent_id)) AS distinct_channel_in_agent_id,
        FIRST_VALUE(requester_id) OVER (PARTITION BY agent_tier, agent_id ORDER BY requester_id ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_requester_id,
        COUNT(*) OVER (PARTITION BY agent_team, agent_tier) AS rows_in_agent_team_agent_tier,
        LEAD(requester_id) OVER (PARTITION BY csat ORDER BY csat, id) AS next_requester_id,
        NTH_VALUE(csat, 2) OVER (PARTITION BY csat ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_csat,
        RANK() OVER w_csat_agent_tier_by_requester AS requester_id_rank,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY agent_team, agent_tier)) AS distinct_agent_id_in_agent_team_agent_tier,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY agent_team, agent_tier), 0), 4) AS csat_share_of_agent_team_agent_tier,
        MAX(requester_id) OVER (PARTITION BY agent_tier, agent_id ORDER BY requester_id ASC NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS requester_id_max_3
    FROM with_parent
    WINDOW w_csat_agent_tier_by_requester AS (PARTITION BY csat, agent_tier ORDER BY requester_id ASC)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(next_requester_id) IGNORE NULLS OVER (PARTITION BY second_csat ORDER BY requester_id_rank ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_next_requester_id,
        MIN(requester_id_rank) OVER (PARTITION BY distinct_agent_id_in_agent_team_agent_tier, agent_team) AS distinct_agent_id_in_agent_team_agent_tier_agent,
        LAG(second_csat) OVER (ORDER BY next_csat DESC NULLS FIRST, id) AS prev_second_csat,
        SIZE(COLLECT_SET(agent_id) OVER w_distinct_agent_id_in_agent_t) AS distinct_agent_id_in_distinct_agent_id_in_agent_,
        FIRST_VALUE(requester_id_max_3) OVER (PARTITION BY second_csat ORDER BY requester_id_rank ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_requester_id_max_3,
        ROUND(CUME_DIST() OVER (ORDER BY next_csat DESC NULLS FIRST), 3) AS next_csat_cume_dist
    FROM ranked
    WINDOW w_distinct_agent_id_in_agent_t AS (PARTITION BY distinct_agent_id_in_agent_team_agent_tier, agent_team)
),
totals AS (
    -- Summarise per group
    SELECT
        distinct_agent_id_in_agent_team_agent_tier,
        second_csat,
        distinct_channel_in_agent_id,
        ARRAY_SORT(COLLECT_SET(agent_name)) AS agent_name_list,
        ROUND(SUM(distinct_channel_in_agent_team_agent_tier) / NULLIF(SUM(requester_id_rank), 0), 3) AS distinct_channel_in_agent_team_agent_tier_to_req,
        ARRAY_SORT(COLLECT_SET(agent_team)) AS agent_team_list,
        COUNT(*) AS row_count,
        ROUND(SUM(first_requester_id) / NULLIF(SUM(requester_id_rank), 0), 3) AS first_requester_id_to_requester_id_rank_ratio,
        COUNT(*) FILTER (WHERE agent_team = 'Tech') AS n_agent_team_tech
    FROM metrics
    GROUP BY distinct_agent_id_in_agent_team_agent_tier, second_csat, distinct_channel_in_agent_id
    HAVING COUNT(*) >= 3
)
SELECT
    distinct_agent_id_in_agent_team_agent_tier,
    second_csat,
    distinct_channel_in_agent_id,
    agent_name_list,
    agent_team_list,
    row_count,
    first_requester_id_to_requester_id_rank_ratio,
    n_agent_team_tech,
    CASE WHEN row_count >= 8 THEN 'HIGH' WHEN row_count >= 5 THEN 'MEDIUM' ELSE 'LOW' END AS row_count_tier
FROM totals
WHERE SIZE(agent_name_list) >= 1
