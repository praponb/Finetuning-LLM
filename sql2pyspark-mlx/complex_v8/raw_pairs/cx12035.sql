WITH filtered AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        tier,
        hired_on,
        DATE_TRUNC('YEAR', hired_on) AS hired_on_year
    FROM support_agents
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_all_by_hired_on_year, 3) AS hired_on_year_percent_rank,
        LAG(hired_on_year) OVER (PARTITION BY tier, YEAR(hired_on) ORDER BY hired_on_year, id) AS prev_hired_on_year,
        ROUND((UNIX_TIMESTAMP(hired_on_year) - UNIX_TIMESTAMP(LAG(hired_on_year) OVER (PARTITION BY tier, YEAR(hired_on) ORDER BY hired_on_year, id))) / 60, 1) AS minutes_since_prev_hired_on_year,
        ROUND(PERCENT_RANK() OVER (ORDER BY hired_on DESC), 3) AS hired_on_percent_rank,
        DATEDIFF(hired_on, LAG(hired_on) OVER (ORDER BY hired_on_year ASC, id)) AS days_since_prev_hired_on,
        ROUND(CUME_DIST() OVER (ORDER BY hired_on_year ASC, id), 3) AS hired_on_year_cume_dist,
        NTILE(3) OVER (ORDER BY hired_on DESC, id) AS hired_on_tercile,
        ROW_NUMBER() OVER (PARTITION BY tier, YEAR(hired_on) ORDER BY hired_on_year, id) AS hired_on_year_seq,
        ROW_NUMBER() OVER (ORDER BY hired_on_year ASC, id) AS hired_on_year_seq_2,
        NTH_VALUE(hired_on, 2) OVER (ORDER BY hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_hired_on,
        ROUND((UNIX_TIMESTAMP(hired_on_year) - UNIX_TIMESTAMP(LAG(hired_on_year) OVER w_all_by_hired_on_year)) / 60, 1) AS minutes_since_prev_hired_on_year_2,
        ROUND(PERCENT_RANK() OVER (ORDER BY hired_on DESC, id), 3) AS hired_on_percent_rank_2
    FROM filtered
    WINDOW w_all_by_hired_on_year AS (ORDER BY hired_on_year ASC, id)
),
first_rows AS (
    -- Top 5 per group
    SELECT
        id,
        agent_name,
        tier,
        hired_on,
        hired_on_year,
        hired_on_year_percent_rank,
        prev_hired_on_year,
        minutes_since_prev_hired_on_year,
        hired_on_percent_rank,
        days_since_prev_hired_on,
        hired_on_year_cume_dist,
        hired_on_tercile,
        hired_on_year_seq_2,
        second_hired_on,
        minutes_since_prev_hired_on_year_2,
        hired_on_percent_rank_2
    FROM with_trends
    WHERE hired_on_year_seq <= 5
),
stats AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', prev_hired_on_year) AS prev_hired_on_year_month,
        DATE_FORMAT(hired_on_year, 'yyyy-MM') AS hired_on_year_ym,
        ROUND(AVG(hired_on_tercile), 2) AS avg_hired_on_tercile,
        ARRAY_SORT(COLLECT_SET(tier)) AS tier_list,
        COUNT(DISTINCT tier) AS distinct_tiers,
        SUM(hired_on_year_percent_rank) AS total_hired_on_year_percent_rank,
        COUNT(*) AS cnt
    FROM first_rows
    GROUP BY DATE_TRUNC('MONTH', prev_hired_on_year), DATE_FORMAT(hired_on_year, 'yyyy-MM')
)
SELECT
    prev_hired_on_year_month,
    hired_on_year_ym,
    distinct_tiers,
    total_hired_on_year_percent_rank,
    cnt,
    CASE WHEN cnt >= 9 THEN 'large' WHEN cnt >= 4 THEN 'medium' ELSE 'small' END AS cnt_tier,
    QUARTER(prev_hired_on_year_month) AS prev_hired_on_year_month_quarter
FROM stats
