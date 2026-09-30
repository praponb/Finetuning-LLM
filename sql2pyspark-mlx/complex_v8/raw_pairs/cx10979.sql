WITH prepared AS (
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
        tags,
        CASE WHEN requester_id >= 14 THEN 'large' WHEN requester_id >= 8 THEN 'medium' ELSE 'small' END AS requester_id_tier,
        SUBSTRING(TRIM(channel), 1, 2) AS channel_prefix,
        YEAR(opened_at) AS opened_at_year
    FROM tickets
    WHERE csat <= 5
      AND ARRAY_CONTAINS(tags, 'bug')
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY requester_id_tier, channel)) AS distinct_agent_id_in_requester_id_tier_channel,
        ROUND(PERCENT_RANK() OVER w_all_by_opened_at_ties, 3) AS opened_at_percent_rank,
        RANK() OVER (ORDER BY opened_at DESC, id) AS opened_at_rank,
        DENSE_RANK() OVER w_all_by_closed_at_ties AS closed_at_dense_rank
    FROM prepared
    WINDOW w_all_by_opened_at_ties AS (ORDER BY opened_at DESC), w_all_by_closed_at_ties AS (ORDER BY closed_at ASC)
),
qualified AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
        )
      AND s.agent_id <= 4
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.channel,
        s.csat,
        s.tags,
        s.channel_prefix,
        s.opened_at_year,
        s.opened_at_percent_rank,
        s.opened_at_rank,
        su.tier AS agent_tier,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on
    FROM qualified s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.team IN ('Tech', 'Onboarding')
),
agent_id_stats AS (
    -- Median opened_at_percent_rank per agent_id
    SELECT agent_id, PERCENTILE_APPROX(opened_at_percent_rank, 0.5) AS median_opened_at_percent_rank
    FROM combined_details
    GROUP BY agent_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_opened_at_percent_rank,
        ROUND(s.opened_at_percent_rank - g.median_opened_at_percent_rank, 2) AS opened_at_percent_rank_minus_median
    FROM combined_details s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.opened_at_percent_rank >= g.median_opened_at_percent_rank
),
stats AS (
    -- Group-level totals
    SELECT
        agent_id,
        DATE_FORMAT(agent_hired_on, 'yyyy-MM') AS agent_hired_on_ym,
        YEAR(opened_at) AS opened_at_year_2,
        ROUND(SUM(opened_at_percent_rank_minus_median), 2) AS total_opened_at_percent_rank_minus_median,
        MIN(opened_at_year) AS min_opened_at_year,
        MAX(opened_at_rank) AS max_opened_at_rank,
        ROUND(AVG(csat), 2) AS avg_csat,
        ARRAY_SORT(COLLECT_SET(agent_team)) AS agent_team_list
    FROM compared
    GROUP BY agent_id, DATE_FORMAT(agent_hired_on, 'yyyy-MM'), YEAR(opened_at)
)
SELECT
    agent_id,
    agent_hired_on_ym,
    min_opened_at_year,
    max_opened_at_rank,
    avg_csat,
    agent_team_list,
    INITCAP(TRIM(agent_hired_on_ym)) AS agent_hired_on_ym_clean,
    SUBSTRING(TRIM(agent_hired_on_ym), 1, 2) AS agent_hired_on_ym_prefix
FROM stats
ORDER BY max_opened_at_rank
