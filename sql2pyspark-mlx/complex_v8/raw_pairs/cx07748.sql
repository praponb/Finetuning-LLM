WITH scoped AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        COALESCE(channel, 'UNKNOWN') AS channel_or_unknown,
        ARRAY_JOIN(ARRAY_SORT(tags), ',') AS tags_text
    FROM tickets
    WHERE csat >= 2
      AND closed_at < TIMESTAMP '2025-02-24 13:00:00'
),
without_matches AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM scoped s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.channel IN ('phone', 'web')
              AND SIZE(t.tags) >= 2
        )
      AND s.csat BETWEEN 2 AND 4
),
combined_details_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.tags,
        s.channel_or_unknown,
        s.tags_text,
        su.team AS agent_team
    FROM without_matches s
    JOIN support_agents su ON su.id = s.agent_id
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        FLOOR(requester_id / 5) * 5 AS requester_id_bucket,
        LOWER(REGEXP_REPLACE(TRIM(channel_or_unknown), ' ', '_')) AS channel_or_unknown_slug,
        DATE_FORMAT(opened_at, 'MMM yyyy') AS opened_at_label
    FROM combined_details_agent
    WHERE opened_at >= TIMESTAMP '2025-02-06 11:00:00'
),
per_group AS (
    -- Summarise per group
    SELECT
        priority,
        agent_team,
        COUNT(DISTINCT agent_team) AS distinct_agent_teams,
        ROUND(SUM(requester_id) / NULLIF(SUM(requester_id_bucket), 0), 3) AS requester_id_to_requester_id_bucket_ratio
    FROM tagged
    GROUP BY priority, agent_team
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(distinct_agent_teams) OVER (PARTITION BY distinct_agent_teams ORDER BY requester_id_to_requester_id_bucket_ratio, priority, agent_team ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS distinct_agent_teams_avg_1_prior,
        ROUND(distinct_agent_teams - AVG(distinct_agent_teams) OVER (PARTITION BY agent_team), 2) AS distinct_agent_teams_vs_agent_team_avg,
        ROUND(AVG(distinct_agent_teams) OVER (PARTITION BY distinct_agent_teams ORDER BY requester_id_to_requester_id_bucket_ratio, priority, agent_team ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS distinct_agent_teams_avg_2_prior,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY agent_team)) AS distinct_priority_in_agent_team
    FROM per_group
),
tagged_2 AS (
    -- Derive helper columns
    SELECT
        priority,
        agent_team,
        distinct_agent_teams,
        distinct_agent_teams_avg_1_prior,
        distinct_agent_teams_vs_agent_team_avg,
        distinct_agent_teams_avg_2_prior,
        distinct_priority_in_agent_team,
        ABS(distinct_agent_teams - 5) AS distinct_agent_teams_distance
    FROM ranked
    WHERE agent_team <> 'Onboarding'
)
SELECT
    distinct_agent_teams,
    distinct_priority_in_agent_team,
    priority,
    ARRAY_SORT(COLLECT_SET(agent_team)) AS agent_team_list,
    SUM(CASE WHEN agent_team NOT IN ('Onboarding', 'Billing') THEN 1 ELSE 0 END) AS n_agent_team_not_in_onboarding,
    COUNT(DISTINCT priority) AS distinct_prioritys
FROM tagged_2
GROUP BY GROUPING SETS ((distinct_agent_teams, distinct_priority_in_agent_team, priority), (distinct_agent_teams), ())
ORDER BY distinct_agent_teams NULLS LAST, distinct_priority_in_agent_team NULLS LAST, priority NULLS LAST
