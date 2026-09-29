WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on
    FROM support_agents
    WHERE team <> 'Billing'
      AND YEAR(hired_on) = 2024
),
summary AS (
    -- Summarise per group
    SELECT
        tier,
        COUNT(*) AS cnt,
        COUNT(*) FILTER (WHERE team IN ('Onboarding', 'Billing', 'Tech')) AS n_team_in_onboarding_billing_t,
        COUNT(DISTINCT team) AS distinct_teams,
        DATEDIFF(MAX(hired_on), MIN(hired_on)) AS hired_on_span_days
    FROM cleaned
    GROUP BY 1
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        tier,
        distinct_teams,
        hired_on_span_days,
        SUM(cnt) OVER (PARTITION BY n_team_in_onboarding_billing_t) AS n_team_in_onboarding_billing_t_sum_cnt,
        ROW_NUMBER() OVER (ORDER BY distinct_teams, tier) AS seq_no
    FROM summary
),
tagged AS (
    -- Add calculated columns
    SELECT
        tier,
        n_team_in_onboarding_billing_t_sum_cnt,
        seq_no,
        CASE WHEN tier IN ('T3', 'T2') THEN 'group_a' WHEN tier = 'T1' THEN 'group_b' ELSE 'other' END AS tier_group,
        ROUND(seq_no / NULLIF(distinct_teams, 0), 3) AS seq_no_per_distinct_teams,
        COALESCE(hired_on_span_days, 0) AS hired_on_span_days_filled
    FROM with_trends
),
tier_group_stats AS (
    -- Avg n_team_in_onboarding_billing_t_sum_cnt per tier_group
    SELECT tier_group, AVG(n_team_in_onboarding_billing_t_sum_cnt) AS avg_n_team_in_onboarding_billing_t_sum_cnt
    FROM tagged
    GROUP BY tier_group
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_n_team_in_onboarding_billing_t_sum_cnt,
        ROUND(s.n_team_in_onboarding_billing_t_sum_cnt - g.avg_n_team_in_onboarding_billing_t_sum_cnt, 2) AS n_team_in_onboarding_billing_t_sum_cnt_minus_avg
    FROM tagged s
    JOIN tier_group_stats g ON g.tier_group = s.tier_group
    WHERE s.n_team_in_onboarding_billing_t_sum_cnt > g.avg_n_team_in_onboarding_billing_t_sum_cnt
)
-- Final projection
SELECT
    tier,
    n_team_in_onboarding_billing_t_sum_cnt,
    tier_group,
    seq_no_per_distinct_teams,
    hired_on_span_days_filled,
    avg_n_team_in_onboarding_billing_t_sum_cnt,
    LOWER(REGEXP_REPLACE(TRIM(tier), ' ', '_')) AS tier_slug
FROM compared
WHERE n_team_in_onboarding_billing_t_sum_cnt_minus_avg BETWEEN -36.0 AND 48.0
ORDER BY seq_no_per_distinct_teams, tier
LIMIT 5
