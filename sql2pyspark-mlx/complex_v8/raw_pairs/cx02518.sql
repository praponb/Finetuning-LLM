WITH cleaned AS (
    -- Filter support_agents
    SELECT
        id,
        team,
        tier,
        hired_on,
        CASE WHEN tier IN ('T1', 'T3') THEN 'group_a' WHEN tier = 'T2' THEN 'group_b' ELSE 'other' END AS tier_group,
        DATEDIFF(DATE '2025-12-31', hired_on) AS days_since_hired_on
    FROM support_agents
    WHERE tier = 'T1'
      AND hired_on BETWEEN DATE '2024-07-09' AND DATE '2025-03-30'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        team,
        tier_group,
        days_since_hired_on,
        MAX(days_since_hired_on) OVER (PARTITION BY team, tier) AS team_tier_max_days_since_hired_on,
        LEAD(days_since_hired_on) OVER (ORDER BY hired_on, id) AS next_days_since_hired_on,
        COUNT(*) OVER (PARTITION BY team, tier) AS rows_in_team_tier,
        DENSE_RANK() OVER (ORDER BY hired_on) AS hired_on_dense_rank,
        ROUND(days_since_hired_on / NULLIF(SUM(days_since_hired_on) OVER (PARTITION BY team, tier), 0), 4) AS days_since_hired_on_share_of_team_tier,
        FIRST_VALUE(days_since_hired_on) IGNORE NULLS OVER (ORDER BY hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_days_since_hired_on
    FROM cleaned
)
-- Subtotals with ROLLUP
SELECT
    hired_on_dense_rank,
    tier_group,
    team,
    SUM(team_tier_max_days_since_hired_on) FILTER (WHERE team = 'VIP') AS team_tier_max_days_since_hired_on_team_vip,
    SUM(rows_in_team_tier) AS total_rows_in_team_tier
FROM with_trends
GROUP BY ROLLUP(hired_on_dense_rank, tier_group, team)
ORDER BY hired_on_dense_rank NULLS LAST, tier_group NULLS LAST, team NULLS LAST
