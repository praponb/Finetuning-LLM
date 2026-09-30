WITH scoped_agents AS (
    -- Filter support_agents
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        LOWER(REGEXP_REPLACE(TRIM(tier), ' ', '_')) AS tier_slug,
        LOWER(REGEXP_REPLACE(TRIM(team), ' ', '_')) AS team_slug
    FROM support_agents
    WHERE LENGTH(TRIM(agent_name)) >= 5
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(4) OVER (ORDER BY hired_on, id) AS hired_on_quartile,
        ROUND(CUME_DIST() OVER (ORDER BY hired_on, id), 3) AS hired_on_cume_dist,
        RANK() OVER w_hired_on_month_by_hired_on_t AS hired_on_rank,
        LEAD(hired_on, 3) OVER (PARTITION BY MONTH(hired_on) ORDER BY hired_on DESC, id) AS next3_hired_on,
        ROW_NUMBER() OVER (PARTITION BY MONTH(hired_on) ORDER BY hired_on DESC, id) AS hired_on_seq,
        RANK() OVER w_tier_by_hired_on_ties AS hired_on_rank_2,
        FIRST_VALUE(hired_on) IGNORE NULLS OVER (PARTITION BY tier ORDER BY hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_hired_on,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(hired_on) ORDER BY hired_on DESC), 3) AS hired_on_cume_dist_2,
        NTH_VALUE(hired_on, 2) OVER (PARTITION BY YEAR(hired_on) ORDER BY hired_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_hired_on,
        DENSE_RANK() OVER (PARTITION BY YEAR(hired_on) ORDER BY hired_on ASC) AS hired_on_dense_rank
    FROM scoped_agents
    WINDOW w_hired_on_month_by_hired_on_t AS (PARTITION BY MONTH(hired_on) ORDER BY hired_on DESC), w_tier_by_hired_on_ties AS (PARTITION BY tier ORDER BY hired_on DESC)
),
top_per_group AS (
    -- Top 2 per group
    SELECT *
    FROM ranked
    WHERE hired_on_seq <= 2
      AND next3_hired_on BETWEEN DATE '2024-01-30' AND DATE '2025-11-09'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(hired_on_cume_dist) OVER (PARTITION BY hired_on_seq, YEAR(next3_hired_on)) AS hired_on_seq_next3_hired_on_year_min_hired_on_cu,
        ROUND(PERCENT_RANK() OVER (ORDER BY hired_on ASC, id), 3) AS hired_on_percent_rank,
        SIZE(COLLECT_SET(team) OVER w_hired_on_seq_next3_hired_on) AS distinct_team_in_hired_on_seq_next3_hired_on_yea,
        DATEDIFF(first_known_hired_on, LAG(first_known_hired_on) OVER (PARTITION BY hired_on_rank ORDER BY hired_on DESC, id)) AS days_since_prev_first_known_hired_on,
        ROUND(hired_on_quartile / NULLIF(SUM(hired_on_quartile) OVER (PARTITION BY hired_on_seq, YEAR(next3_hired_on)), 0), 4) AS hired_on_quartile_share_of_hired_on_seq_next3_hi,
        MAX(hired_on_rank) OVER (PARTITION BY hired_on_rank, team ORDER BY first_known_hired_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_hired_on_rank_so_far,
        ROUND(hired_on_dense_rank / NULLIF(SUM(hired_on_dense_rank) OVER w_hired_on_rank_2_hired_on_den, 0), 4) AS hired_on_dense_rank_share_of_hired_on_rank_2_hir
    FROM top_per_group
    WINDOW w_hired_on_seq_next3_hired_on AS (PARTITION BY hired_on_seq, YEAR(next3_hired_on)), w_hired_on_rank_2_hired_on_den AS (PARTITION BY hired_on_rank_2, hired_on_dense_rank)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', first_known_hired_on) AS first_known_hired_on_month,
        distinct_team_in_hired_on_seq_next3_hired_on_yea,
        hired_on_seq,
        SUM(days_since_prev_first_known_hired_on) AS total_days_since_prev_first_known_hired_on,
        ROUND(AVG(hired_on_rank), 2) AS avg_hired_on_rank
    FROM with_trends
    GROUP BY 1, 2, 3
    HAVING SUM(hired_on_rank_2) > 2
)
SELECT
    first_known_hired_on_month,
    distinct_team_in_hired_on_seq_next3_hired_on_yea,
    hired_on_seq,
    total_days_since_prev_first_known_hired_on
FROM stats
