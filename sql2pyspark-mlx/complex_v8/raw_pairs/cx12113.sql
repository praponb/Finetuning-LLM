WITH scoped_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        csat,
        tags,
        DATEDIFF(DATE '2026-01-01', opened_at) AS days_since_opened_at
    FROM tickets
    WHERE requester_id >= 7
      AND channel NOT IN ('chat', 'web')
),
enriched AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.tags,
        s.days_since_opened_at,
        su.team AS agent_team,
        su.agent_name
    FROM scoped_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        agent_id,
        opened_at,
        tags,
        days_since_opened_at,
        agent_team,
        agent_name,
        RANK() OVER (PARTITION BY agent_team, agent_id ORDER BY days_since_opened_at ASC, id) AS days_since_opened_at_rank,
        FIRST_VALUE(days_since_opened_at) OVER (ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_days_since_opened_at,
        NTH_VALUE(opened_at, 2) OVER (PARTITION BY agent_id, DATE_TRUNC('MONTH', opened_at) ORDER BY days_since_opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_opened_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY agent_team, agent_id ORDER BY days_since_opened_at ASC, id), 3) AS days_since_opened_at_percent_rank,
        SUM(days_since_opened_at) OVER (PARTITION BY agent_id) AS agent_id_sum_days_since_opened_at,
        ROUND(AVG(days_since_opened_at) OVER (PARTITION BY agent_id, agent_team ORDER BY opened_at NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS days_since_opened_at_avg_2,
        NTILE(5) OVER w_agent_team_agent_id_by_days AS days_since_opened_at_quintile,
        SIZE(COLLECT_SET(agent_team) OVER (PARTITION BY agent_id)) AS distinct_agent_team_in_agent_id,
        ROUND(days_since_opened_at / NULLIF(SUM(days_since_opened_at) OVER (PARTITION BY agent_id, agent_team), 0), 4) AS days_since_opened_at_share_of_agent_id_agent_tea,
        LEAD(days_since_opened_at) OVER w_agent_team_agent_id_by_days AS next_days_since_opened_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY agent_team ORDER BY opened_at ASC, id), 3) AS opened_at_percent_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY agent_team, agent_id ORDER BY days_since_opened_at ASC), 3) AS days_since_opened_at_percent_rank_2,
        ROUND(CUME_DIST() OVER w_agent_team_by_opened_at_ties, 3) AS opened_at_cume_dist
    FROM enriched
    WINDOW w_agent_team_agent_id_by_days AS (PARTITION BY agent_team, agent_id ORDER BY days_since_opened_at ASC, id), w_agent_team_by_opened_at_ties AS (PARTITION BY agent_team ORDER BY opened_at ASC)
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        agent_id,
        opened_at,
        tags,
        agent_name,
        first_days_since_opened_at,
        second_opened_at,
        days_since_opened_at_percent_rank,
        days_since_opened_at_avg_2,
        days_since_opened_at_quintile,
        next_days_since_opened_at,
        opened_at_percent_rank,
        days_since_opened_at_percent_rank_2,
        opened_at_cume_dist,
        COALESCE(agent_team, 'UNKNOWN') AS agent_team_or_unknown,
        opened_at + INTERVAL 30 MINUTES AS opened_at_plus_30m,
        CASE WHEN days_since_opened_at >= 420 THEN 'A' WHEN days_since_opened_at >= 210 THEN 'B' ELSE 'C' END AS days_since_opened_at_tier
    FROM metrics
),
per_group AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', opened_at) AS opened_at_month,
        DATE_TRUNC('MONTH', opened_at_plus_30m) AS opened_at_plus_30m_month,
        days_since_opened_at_quintile,
        ARRAY_SORT(COLLECT_SET(agent_name)) AS agent_name_list,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS cnt,
        SUM(CASE WHEN LENGTH(TRIM(agent_name)) >= 7 THEN days_since_opened_at_quintile ELSE 0 END) AS days_since_opened_at_quintile_length_trim_agent_,
        COUNT(DISTINCT agent_team_or_unknown) AS distinct_agent_team_or_unknowns
    FROM tagged
    GROUP BY DATE_TRUNC('MONTH', opened_at), DATE_TRUNC('MONTH', opened_at_plus_30m), days_since_opened_at_quintile
)
SELECT
    opened_at_month,
    opened_at_plus_30m_month,
    agent_name_list,
    distinct_ids,
    cnt,
    days_since_opened_at_quintile_length_trim_agent_,
    distinct_agent_team_or_unknowns,
    COALESCE(cnt, 0) AS cnt_filled
FROM per_group
ORDER BY distinct_agent_team_or_unknowns
