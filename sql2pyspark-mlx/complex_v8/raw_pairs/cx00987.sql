WITH recent_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        tags,
        TO_DATE(closed_at) AS closed_at_date
    FROM tickets
    WHERE csat <= 3
      AND channel NOT IN ('chat', 'phone')
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.tags,
        s.closed_at_date,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM recent_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(closed_at) IGNORE NULLS OVER (PARTITION BY agent_tier, agent_team ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_closed_at,
        NTILE(4) OVER (PARTITION BY agent_team ORDER BY closed_at ASC, id) AS closed_at_quartile,
        LAST_VALUE(closed_at_date) IGNORE NULLS OVER (PARTITION BY agent_team ORDER BY closed_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_closed_at_date,
        LAG(closed_at_date) OVER w_agent_tier_by_closed_at AS prev_closed_at_date,
        LAST_VALUE(closed_at_date) OVER (PARTITION BY agent_tier ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_closed_at_date,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(LAG(closed_at) OVER w_all_by_closed_at_date)) / 60, 1) AS minutes_since_prev_closed_at,
        FIRST_VALUE(closed_at_date) IGNORE NULLS OVER (PARTITION BY agent_tier, agent_team ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_closed_at_date,
        DENSE_RANK() OVER (PARTITION BY agent_tier ORDER BY closed_at, id) AS closed_at_dense_rank,
        DENSE_RANK() OVER (PARTITION BY agent_team ORDER BY closed_at ASC, id) AS closed_at_dense_rank_2,
        FIRST_VALUE(closed_at) IGNORE NULLS OVER (PARTITION BY agent_team ORDER BY closed_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_closed_at_2,
        DENSE_RANK() OVER (PARTITION BY agent_tier, agent_team ORDER BY closed_at) AS closed_at_dense_rank_3,
        RANK() OVER (PARTITION BY agent_tier ORDER BY closed_at, id) AS closed_at_rank
    FROM combined_details
    WINDOW w_agent_tier_by_closed_at AS (PARTITION BY agent_tier ORDER BY closed_at, id), w_all_by_closed_at_date AS (ORDER BY closed_at_date DESC NULLS FIRST, id)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(closed_at_dense_rank - AVG(closed_at_dense_rank) OVER (PARTITION BY DATE_TRUNC('MONTH', prev_closed_at_date), closed_at_rank), 2) AS closed_at_dense_rank_vs_prev_closed_at_date_mont,
        SIZE(COLLECT_SET(agent_team) OVER (PARTITION BY closed_at_dense_rank_3)) AS distinct_agent_team_in_closed_at_dense_rank_3,
        SIZE(COLLECT_SET(agent_team) OVER (PARTITION BY DATE_TRUNC('MONTH', prev_closed_at_date), closed_at_rank)) AS distinct_agent_team_in_prev_closed_at_date_month,
        minutes_since_prev_closed_at - LAG(minutes_since_prev_closed_at) OVER (ORDER BY last_closed_at_date, id) AS minutes_since_prev_closed_at_change,
        MAX(closed_at_dense_rank) OVER (PARTITION BY YEAR(closed_at) ORDER BY closed_at_dense_rank_3 ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS closed_at_dense_rank_max_6_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(last_known_closed_at_date) ORDER BY closed_at_dense_rank ASC), 3) AS closed_at_dense_rank_percent_rank
    FROM metrics
),
grouped AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(first_known_closed_at_2, 'yyyy-MM') AS first_known_closed_at_2_ym,
        closed_at_dense_rank,
        COUNT(*) AS cnt,
        ROUND(STDDEV(closed_at_dense_rank_vs_prev_closed_at_date_mont), 2) AS stddev_closed_at_dense_rank_vs_prev_closed_at_da,
        MAX(first_known_closed_at) AS last_first_known_closed_at,
        COUNT(DISTINCT agent_team) AS distinct_agent_teams,
        COUNT(DISTINCT agent_tier) AS distinct_agent_tiers
    FROM with_trends
    WHERE closed_at_dense_rank_2 >= 2
    GROUP BY DATE_FORMAT(first_known_closed_at_2, 'yyyy-MM'), closed_at_dense_rank
)
SELECT
    first_known_closed_at_2_ym,
    closed_at_dense_rank,
    cnt,
    stddev_closed_at_dense_rank_vs_prev_closed_at_da,
    last_first_known_closed_at,
    distinct_agent_teams,
    distinct_agent_tiers,
    FLOOR(closed_at_dense_rank / 2) * 2 AS closed_at_dense_rank_bucket
FROM grouped
WHERE stddev_closed_at_dense_rank_vs_prev_closed_at_da >= 3.5
ORDER BY distinct_agent_teams DESC, first_known_closed_at_2_ym DESC
