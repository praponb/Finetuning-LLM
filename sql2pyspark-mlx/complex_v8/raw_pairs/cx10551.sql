WITH valid_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE HOUR(closed_at) BETWEEN 9 AND 15
      AND requester_id > 2
),
with_details AS (
    -- Join support_agents
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
        su.hired_on AS agent_hired_on,
        su.team AS agent_team,
        su.tier AS agent_tier
    FROM valid_tickets s
    JOIN support_agents su ON su.id = s.agent_id AND LOWER(su.agent_name) LIKE '%wa%'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags,
        agent_team,
        agent_tier,
        DENSE_RANK() OVER (PARTITION BY channel ORDER BY csat, id) AS csat_dense_rank,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(LAG(closed_at) OVER (PARTITION BY channel ORDER BY csat, id))) / 60, 1) AS minutes_since_prev_closed_at,
        requester_id - LAG(requester_id) OVER w_agent_team_channel_by_opened AS requester_id_change,
        ROUND(csat - AVG(csat) OVER (PARTITION BY YEAR(closed_at), priority), 2) AS csat_vs_closed_at_year_priority_avg,
        FIRST_VALUE(opened_at) IGNORE NULLS OVER (PARTITION BY channel ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_opened_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY agent_team, channel ORDER BY opened_at), 3) AS opened_at_percent_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY channel ORDER BY csat), 3) AS csat_percent_rank,
        SIZE(COLLECT_SET(agent_team) OVER (PARTITION BY YEAR(closed_at), priority)) AS distinct_agent_team_in_closed_at_year_priority,
        MIN(requester_id) OVER w_closed_at_year_priority AS closed_at_year_priority_min_requester_id,
        LAST_VALUE(closed_at) OVER (PARTITION BY agent_tier, csat ORDER BY agent_hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_closed_at,
        NTH_VALUE(opened_at, 2) OVER (PARTITION BY agent_tier, csat ORDER BY agent_hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_at,
        COUNT(*) OVER (PARTITION BY YEAR(closed_at), priority) AS rows_in_closed_at_year_priority,
        ROUND(PERCENT_RANK() OVER (PARTITION BY agent_tier, csat ORDER BY agent_hired_on, id), 3) AS agent_hired_on_percent_rank,
        NTILE(10) OVER w_agent_team_channel_by_opened AS opened_at_decile,
        ROUND(CUME_DIST() OVER (PARTITION BY agent_team, channel ORDER BY opened_at, id), 3) AS opened_at_cume_dist
    FROM with_details
    WINDOW w_agent_team_channel_by_opened AS (PARTITION BY agent_team, channel ORDER BY opened_at, id), w_closed_at_year_priority AS (PARTITION BY YEAR(closed_at), priority)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        agent_id,
        priority,
        tags,
        agent_team,
        agent_tier,
        requester_id_change,
        first_known_opened_at,
        opened_at_percent_rank,
        csat_percent_rank,
        distinct_agent_team_in_closed_at_year_priority,
        last_closed_at,
        agent_hired_on_percent_rank,
        ROUND(opened_at_decile - AVG(opened_at_decile) OVER w_opened_at_decile, 2) AS opened_at_decile_vs_opened_at_decile_avg,
        SUM(minutes_since_prev_closed_at) OVER (PARTITION BY priority, channel ORDER BY last_closed_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_minutes_since_prev_closed_at,
        ROUND(opened_at_cume_dist / NULLIF(SUM(opened_at_cume_dist) OVER (PARTITION BY opened_at_decile), 0), 4) AS opened_at_cume_dist_share_of_opened_at_decile,
        ROUND(requester_id_change / NULLIF(SUM(requester_id_change) OVER (PARTITION BY opened_at_decile), 0), 4) AS requester_id_change_share_of_opened_at_decile
    FROM with_trends
    WINDOW w_opened_at_decile AS (PARTITION BY opened_at_decile)
)
-- Final projection
SELECT
    id,
    agent_id,
    priority,
    tags,
    agent_team,
    agent_tier,
    requester_id_change,
    first_known_opened_at,
    opened_at_percent_rank,
    csat_percent_rank,
    distinct_agent_team_in_closed_at_year_priority,
    agent_hired_on_percent_rank,
    opened_at_decile_vs_opened_at_decile_avg,
    running_minutes_since_prev_closed_at,
    requester_id_change_share_of_opened_at_decile,
    DATEDIFF(DATE '2025-12-31', last_closed_at) AS days_since_last_closed_at
FROM sequenced
ORDER BY days_since_last_closed_at DESC, requester_id_change NULLS LAST, id
LIMIT 5
