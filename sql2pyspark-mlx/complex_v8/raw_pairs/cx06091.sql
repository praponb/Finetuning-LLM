WITH recent_agents AS (
    -- Filter support_agents
    SELECT
        id,
        agent_name,
        team,
        tier,
        COALESCE(team, 'UNKNOWN') AS team_or_unknown,
        LOWER(REGEXP_REPLACE(TRIM(agent_name), ' ', '_')) AS agent_name_slug,
        DATE_FORMAT(hired_on, 'yyyy-MM-dd') AS hired_on_label
    FROM support_agents
    WHERE LOWER(agent_name) LIKE '%t %'
      AND tier = 'T1'
),
totals AS (
    -- Summarise per group
    SELECT
        tier,
        team_or_unknown,
        ROUND(100 * COUNT(CASE WHEN tier = 'T2' THEN 1 END) / COUNT(*), 1) AS pct_tier_t2,
        COUNT(DISTINCT tier) AS distinct_tiers,
        ARRAY_SORT(COLLECT_SET(team)) AS team_list,
        COUNT(*) AS n_rows,
        ARRAY_SORT(COLLECT_SET(tier)) AS tier_list
    FROM recent_agents
    GROUP BY 1, 2
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN pct_tier_t2 >= 85.0 THEN 'HIGH' WHEN pct_tier_t2 >= 25.0 THEN 'MEDIUM' ELSE 'LOW' END AS pct_tier_t2_tier,
        FLOOR(n_rows / 2) * 2 AS n_rows_bucket,
        LOWER(REGEXP_REPLACE(TRIM(team_or_unknown), ' ', '_')) AS team_or_unknown_slug
    FROM totals
    WHERE SIZE(tier_list) >= 1
),
exploded AS (
    -- One row per tier_list_item
    SELECT
        s.tier,
        s.team_or_unknown,
        s.pct_tier_t2,
        s.distinct_tiers,
        s.team_list,
        s.n_rows,
        s.pct_tier_t2_tier,
        s.n_rows_bucket,
        s.team_or_unknown_slug,
        t.tier_list_item_pos,
        t.tier_list_item
    FROM derived s
    LATERAL VIEW POSEXPLODE(s.tier_list) t AS tier_list_item_pos, tier_list_item
)
-- Final projection
SELECT
    tier,
    team_or_unknown,
    pct_tier_t2,
    distinct_tiers,
    team_list,
    n_rows,
    tier_list_item_pos,
    tier_list_item
FROM exploded
WHERE tier_list_item_pos > 1
ORDER BY pct_tier_t2
