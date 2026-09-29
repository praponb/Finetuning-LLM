WITH recent AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        QUARTER(hired_on) AS hired_on_quarter,
        DATEDIFF(DATE '2026-01-01', hired_on) AS days_since_hired_on
    FROM support_agents
    WHERE hired_on IS NOT NULL
      AND tier IN ('T1', 'T3')
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER w_hired_on_quarter AS rows_in_hired_on_quarter,
        COUNT(*) OVER (PARTITION BY team, tier) AS rows_in_team_tier,
        MAX(days_since_hired_on) OVER (ORDER BY days_since_hired_on, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS days_since_hired_on_max_2_prior,
        ROUND(AVG(days_since_hired_on) OVER (PARTITION BY team ORDER BY hired_on, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS days_since_hired_on_avg_3,
        ROUND(AVG(hired_on_quarter) OVER (PARTITION BY MONTH(hired_on) ORDER BY days_since_hired_on ASC NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS hired_on_quarter_avg_3_prior,
        RANK() OVER (PARTITION BY team ORDER BY hired_on, id) AS hired_on_rank,
        MAX(days_since_hired_on) OVER (PARTITION BY team, tier) AS team_tier_max_days_since_hired_on,
        LAG(hired_on_quarter, 3, 0) OVER (PARTITION BY MONTH(hired_on) ORDER BY days_since_hired_on ASC NULLS LAST, id) AS prev3_hired_on_quarter,
        SUM(days_since_hired_on) OVER (PARTITION BY team ORDER BY hired_on, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS days_since_hired_on_sum_2_prior
    FROM recent
    WINDOW w_hired_on_quarter AS (PARTITION BY hired_on_quarter)
),
enriched AS (
    -- Label and score rows
    SELECT *, QUARTER(hired_on) AS hired_on_quarter_2
    FROM ranked
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        agent_name,
        hired_on_quarter,
        days_since_hired_on,
        rows_in_hired_on_quarter,
        days_since_hired_on_avg_3,
        hired_on_quarter_avg_3_prior,
        team_tier_max_days_since_hired_on,
        prev3_hired_on_quarter,
        hired_on_quarter_2,
        MAX(days_since_hired_on_avg_3) OVER (PARTITION BY prev3_hired_on_quarter, hired_on_rank ORDER BY rows_in_team_tier ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_days_since_hired_on_avg_3_so_far,
        days_since_hired_on - LAG(days_since_hired_on) OVER w_team_hired_on_quarter_by_day AS days_since_hired_on_change,
        days_since_hired_on_avg_3 - LAG(days_since_hired_on_avg_3) OVER (PARTITION BY team, hired_on_quarter ORDER BY days_since_hired_on_max_2_prior ASC, id) AS days_since_hired_on_avg_3_change
    FROM enriched
    WINDOW w_team_hired_on_quarter_by_day AS (PARTITION BY team, hired_on_quarter ORDER BY days_since_hired_on_max_2_prior ASC, id)
)
-- Final projection
SELECT
    id,
    agent_name,
    rows_in_hired_on_quarter,
    hired_on_quarter_avg_3_prior,
    team_tier_max_days_since_hired_on,
    prev3_hired_on_quarter,
    hired_on_quarter_2,
    max_days_since_hired_on_avg_3_so_far,
    days_since_hired_on_change,
    days_since_hired_on_avg_3_change,
    rows_in_hired_on_quarter - hired_on_quarter AS rows_in_hired_on_quarter_minus_hired_on_quarter
FROM sequenced
ORDER BY days_since_hired_on_change, days_since_hired_on_avg_3_change DESC
