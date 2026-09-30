WITH valid_agents AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        team,
        tier,
        DATE_TRUNC('MONTH', hired_on) AS hired_on_month
    FROM support_agents
    WHERE team = 'VIP'
      AND UPPER(TRIM(agent_name)) LIKE 'G%'
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_TRUNC('QUARTER', hired_on_month) AS hired_on_month_quarter,
        CONCAT_WS(' / ', agent_name, tier) AS agent_name_tier_label,
        CONCAT(tier, '#', CAST(id AS STRING)) AS tier_ref
    FROM valid_agents
    WHERE LOWER(agent_name) LIKE '%an%'
),
summary AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', hired_on_month_quarter) AS hired_on_month_quarter_month,
        tier,
        COUNT(DISTINCT tier) AS distinct_tiers,
        ARRAY_SORT(COLLECT_SET(tier)) AS tier_list,
        SUM(CASE WHEN tier = 'T1' THEN 1 ELSE 0 END) AS n_tier_t1,
        COUNT(*) AS row_count,
        COUNT(*) FILTER (WHERE tier <> 'T3') AS n_tier_t3
    FROM derived
    GROUP BY DATE_TRUNC('MONTH', hired_on_month_quarter), tier
    HAVING COUNT(*) >= 2
),
per_label AS (
    -- One row per tier_list_item
    SELECT
        s.hired_on_month_quarter_month,
        s.tier,
        s.distinct_tiers,
        s.n_tier_t1,
        s.row_count,
        s.n_tier_t3,
        t.tier_list_item_pos,
        t.tier_list_item
    FROM summary s
    LATERAL VIEW POSEXPLODE(s.tier_list) t AS tier_list_item_pos, tier_list_item
)
SELECT
    hired_on_month_quarter_month,
    distinct_tiers,
    tier_list_item_pos,
    tier_list_item,
    UPPER(TRIM(tier_list_item)) AS tier_list_item_clean,
    LEAST(distinct_tiers, n_tier_t1) AS least_distinct_tiers_n_tier_t1
FROM per_label
ORDER BY hired_on_month_quarter_month DESC
