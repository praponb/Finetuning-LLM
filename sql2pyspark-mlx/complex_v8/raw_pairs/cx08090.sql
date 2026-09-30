WITH cleaned AS (
    -- Filter support_agents
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        TRUNC(hired_on, 'MM') AS hired_on_month_start,
        DATE_TRUNC('MONTH', hired_on) AS hired_on_month
    FROM support_agents
    WHERE hired_on < DATE '2025-02-26'
      AND UPPER(TRIM(agent_name)) LIKE 'J%'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY team, DATE_TRUNC('MONTH', hired_on_month) ORDER BY hired_on ASC), 3) AS hired_on_percent_rank,
        DATEDIFF(hired_on_month_start, LAG(hired_on_month_start) OVER w_team_tier_by_hired_on) AS days_since_prev_hired_on_month_start,
        FIRST_VALUE(hired_on_month) OVER (PARTITION BY tier ORDER BY hired_on_month DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_hired_on_month,
        LEAD(hired_on_month) OVER (PARTITION BY tier ORDER BY hired_on_month DESC, id) AS next_hired_on_month,
        NTH_VALUE(hired_on, 2) OVER (PARTITION BY tier ORDER BY hired_on_month DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_hired_on,
        LAG(hired_on) OVER w_hired_on_month_hired_on_mont AS prev_hired_on,
        LAST_VALUE(hired_on_month_start) IGNORE NULLS OVER (ORDER BY hired_on_month, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_hired_on_month_start,
        DENSE_RANK() OVER w_team_tier_by_hired_on_ties AS hired_on_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY team, tier ORDER BY hired_on, id) AS seq_no
    FROM cleaned
    WINDOW w_team_tier_by_hired_on AS (PARTITION BY team, tier ORDER BY hired_on, id), w_hired_on_month_hired_on_mont AS (PARTITION BY MONTH(hired_on), TO_DATE(hired_on_month) ORDER BY hired_on, id), w_team_tier_by_hired_on_ties AS (PARTITION BY team, tier ORDER BY hired_on)
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        DATE_TRUNC('WEEK', first_hired_on_month) AS first_hired_on_month_week
    FROM ranked
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(hired_on_dense_rank) OVER (PARTITION BY YEAR(hired_on_month)) AS hired_on_month_year_min_hired_on_dense_rank,
        ROUND(AVG(hired_on_percent_rank) OVER (PARTITION BY MONTH(next_hired_on_month) ORDER BY second_hired_on, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS hired_on_percent_rank_avg_2,
        ROUND(hired_on_dense_rank - AVG(hired_on_dense_rank) OVER w_hired_on_month_year, 2) AS hired_on_dense_rank_vs_hired_on_month_year_avg,
        ROUND(hired_on_percent_rank - AVG(hired_on_percent_rank) OVER w_next_hired_on_month_month, 2) AS hired_on_percent_rank_vs_next_hired_on_month_mon,
        ROUND(seq_no / NULLIF(SUM(seq_no) OVER (PARTITION BY MONTH(next_hired_on_month)), 0), 4) AS seq_no_share_of_next_hired_on_month_month,
        ROUND(hired_on_dense_rank / NULLIF(SUM(hired_on_dense_rank) OVER w_hired_on_month_year, 0), 4) AS hired_on_dense_rank_share_of_hired_on_month_year
    FROM tagged
    WINDOW w_hired_on_month_year AS (PARTITION BY YEAR(hired_on_month)), w_next_hired_on_month_month AS (PARTITION BY MONTH(next_hired_on_month))
)
-- Final projection
SELECT
    id,
    team,
    tier,
    hired_on,
    hired_on_month_start,
    hired_on_month,
    first_hired_on_month,
    next_hired_on_month,
    second_hired_on,
    prev_hired_on,
    last_known_hired_on_month_start,
    seq_no,
    first_hired_on_month_week,
    hired_on_percent_rank_avg_2,
    hired_on_dense_rank_vs_hired_on_month_year_avg,
    seq_no_share_of_next_hired_on_month_month,
    hired_on_dense_rank_share_of_hired_on_month_year,
    DATEDIFF(second_hired_on, hired_on) AS days_hired_on_to_second_hired_on,
    YEAR(prev_hired_on) AS prev_hired_on_year
FROM metrics
ORDER BY days_hired_on_to_second_hired_on DESC NULLS FIRST
