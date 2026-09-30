WITH cleaned AS (
    -- Scope the support_agents rows
    SELECT
        id,
        team,
        tier,
        hired_on
    FROM support_agents
    WHERE team IN ('Onboarding', 'VIP')
      AND UPPER(TRIM(agent_name)) LIKE 'L%'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        tier,
        hired_on,
        ROUND(CUME_DIST() OVER w_all_by_hired_on_ties, 3) AS hired_on_cume_dist,
        ROUND(PERCENT_RANK() OVER (ORDER BY hired_on DESC), 3) AS hired_on_percent_rank,
        NTH_VALUE(hired_on, 2) OVER (ORDER BY hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_hired_on,
        NTILE(3) OVER w_all_by_hired_on AS hired_on_tercile,
        DATEDIFF(hired_on, LAG(hired_on) OVER (ORDER BY hired_on DESC, id)) AS days_since_prev_hired_on
    FROM cleaned
    WINDOW w_all_by_hired_on_ties AS (ORDER BY hired_on DESC), w_all_by_hired_on AS (ORDER BY hired_on DESC, id)
),
windowed_2 AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        hired_on_cume_dist,
        hired_on_percent_rank,
        second_hired_on,
        hired_on_tercile,
        MAX(hired_on_percent_rank) OVER (PARTITION BY MONTH(second_hired_on), tier ORDER BY second_hired_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_hired_on_percent_rank_so_far,
        ROUND(hired_on_percent_rank / NULLIF(SUM(hired_on_percent_rank) OVER (PARTITION BY MONTH(hired_on), tier), 0), 4) AS hired_on_percent_rank_share_of_hired_on_month_ti,
        ROUND(hired_on_cume_dist - AVG(hired_on_cume_dist) OVER (PARTITION BY MONTH(hired_on), tier), 2) AS hired_on_cume_dist_vs_hired_on_month_tier_avg,
        LAST_VALUE(days_since_prev_hired_on) OVER (ORDER BY second_hired_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_days_since_prev_hired_on,
        ROUND(AVG(hired_on_tercile) OVER (PARTITION BY MONTH(hired_on), tier), 2) AS hired_on_month_tier_avg_hired_on_tercile,
        MAX(days_since_prev_hired_on) OVER (PARTITION BY MONTH(hired_on), tier) AS hired_on_month_tier_max_days_since_prev_hired_on
    FROM windowed
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        hired_on_cume_dist,
        hired_on_percent_rank,
        hired_on_tercile,
        hired_on_percent_rank_share_of_hired_on_month_ti,
        hired_on_month_tier_avg_hired_on_tercile,
        hired_on_month_tier_max_days_since_prev_hired_on,
        ROUND(hired_on_percent_rank / NULLIF(hired_on_tercile, 0), 3) AS hired_on_percent_rank_per_hired_on_tercile,
        DATE_TRUNC('QUARTER', second_hired_on) AS second_hired_on_quarter
    FROM windowed_2
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        hired_on_cume_dist,
        hired_on_percent_rank,
        hired_on_tercile,
        hired_on_month_tier_max_days_since_prev_hired_on,
        hired_on_percent_rank_per_hired_on_tercile,
        second_hired_on_quarter,
        NTILE(3) OVER (PARTITION BY hired_on_tercile ORDER BY second_hired_on_quarter ASC NULLS LAST, id) AS second_hired_on_quarter_tercile,
        COUNT(*) OVER (PARTITION BY hired_on_tercile) AS rows_in_hired_on_tercile,
        RANK() OVER (PARTITION BY hired_on_tercile ORDER BY hired_on_percent_rank NULLS LAST) AS hired_on_percent_rank_rank,
        LAG(second_hired_on_quarter) OVER w_hired_on_tercile_by_hired_on_2 AS prev_second_hired_on_quarter,
        LAST_VALUE(hired_on_cume_dist) OVER (PARTITION BY hired_on_tercile ORDER BY second_hired_on_quarter ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_hired_on_cume_dist
    FROM annotated
    WINDOW w_hired_on_tercile_by_hired_on_2 AS (PARTITION BY hired_on_tercile ORDER BY hired_on_percent_rank NULLS LAST, id)
)
-- Final projection
SELECT
    id,
    hired_on_cume_dist,
    hired_on_tercile,
    second_hired_on_quarter_tercile,
    rows_in_hired_on_tercile,
    hired_on_percent_rank_rank,
    prev_second_hired_on_quarter,
    CASE WHEN hired_on_tercile >= 6 THEN 'A' WHEN hired_on_tercile >= 4 THEN 'B' ELSE 'C' END AS hired_on_tercile_tier
FROM ranked
WHERE hired_on_tercile < 9
ORDER BY rows_in_hired_on_tercile, hired_on_tercile_tier DESC NULLS FIRST
