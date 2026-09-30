WITH scoped AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        CASE WHEN priority IN ('P2', 'P4') THEN 'group_a' WHEN priority = 'P3' THEN 'group_b' ELSE 'other' END AS priority_group,
        COALESCE(channel, 'UNKNOWN') AS channel_or_unknown,
        GREATEST(requester_id, csat) AS greatest_requester_id_csat
    FROM tickets
    WHERE opened_at >= TIMESTAMP '2025-02-06 06:00:00'
      AND agent_id <= 8
),
with_details_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.channel,
        s.csat,
        s.priority_group,
        s.channel_or_unknown,
        s.greatest_requester_id_csat,
        su.team AS agent_team,
        su.tier AS agent_tier,
        su.hired_on AS agent_hired_on
    FROM scoped s
    JOIN support_agents su ON su.id = s.agent_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(greatest_requester_id_csat) OVER (PARTITION BY channel ORDER BY greatest_requester_id_csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_greatest_requester_id_csat,
        SIZE(COLLECT_SET(agent_id) OVER w_agent_team_priority_group) AS distinct_agent_id_in_agent_team_priority_group,
        FIRST_VALUE(csat) IGNORE NULLS OVER (PARTITION BY channel ORDER BY greatest_requester_id_csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_csat,
        NTILE(3) OVER (PARTITION BY channel ORDER BY greatest_requester_id_csat, id) AS greatest_requester_id_csat_tercile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY channel ORDER BY greatest_requester_id_csat), 3) AS greatest_requester_id_csat_percent_rank,
        SUM(greatest_requester_id_csat) OVER (PARTITION BY agent_team, priority_group) AS agent_team_priority_group_sum_greatest_requester
    FROM with_details_agent
    WINDOW w_agent_team_priority_group AS (PARTITION BY agent_team, priority_group)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(first_known_csat) OVER (PARTITION BY priority_group, agent_team ORDER BY greatest_requester_id_csat_tercile ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_first_known_csat_so_far,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY priority_group ORDER BY agent_team_priority_group_sum_greatest_requester ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        RANK() OVER (PARTITION BY priority_group ORDER BY agent_team_priority_group_sum_greatest_requester ASC, id) AS agent_team_priority_group_sum_greatest_requester_2
    FROM metrics
)
SELECT
    csat,
    COALESCE(agent_tier, 'ALL') AS agent_tier,
    GROUPING_ID() AS gid,
    ARRAY_SORT(COLLECT_SET(channel)) AS channel_list,
    ROUND(STDDEV(agent_team_priority_group_sum_greatest_requester_2), 2) AS stddev_agent_team_priority_group_sum_greatest_re,
    ROUND(AVG(greatest_requester_id_csat_tercile), 2) AS avg_greatest_requester_id_csat_tercile
FROM sequenced
GROUP BY ROLLUP(csat, agent_tier)
ORDER BY csat NULLS LAST, agent_tier NULLS LAST
