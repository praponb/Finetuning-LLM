WITH scoped AS (
    -- Filter support_agents
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        CONCAT(team, '#', CAST(id AS STRING)) AS team_ref
    FROM support_agents
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(hired_on) IGNORE NULLS OVER (PARTITION BY team ORDER BY hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_hired_on,
        ROUND(PERCENT_RANK() OVER (PARTITION BY team ORDER BY hired_on), 3) AS hired_on_percent_rank,
        DENSE_RANK() OVER (PARTITION BY team ORDER BY hired_on DESC) AS hired_on_dense_rank,
        LEAD(hired_on) OVER w_team_by_hired_on AS next_hired_on,
        ROUND(PERCENT_RANK() OVER (ORDER BY hired_on), 3) AS hired_on_percent_rank_2,
        RANK() OVER (PARTITION BY team ORDER BY hired_on DESC) AS hired_on_rank,
        LEAD(hired_on, 2) OVER w_team_by_hired_on AS next2_hired_on,
        DATEDIFF(hired_on, LAG(hired_on) OVER w_all_by_hired_on) AS days_since_prev_hired_on,
        LEAD(hired_on) OVER (PARTITION BY team ORDER BY hired_on DESC, id) AS next_hired_on_2,
        LAST_VALUE(hired_on) IGNORE NULLS OVER (ORDER BY hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_hired_on
    FROM scoped
    WINDOW w_team_by_hired_on AS (PARTITION BY team ORDER BY hired_on, id), w_all_by_hired_on AS (ORDER BY hired_on, id)
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        first_known_hired_on,
        hired_on_percent_rank,
        hired_on_percent_rank_2,
        hired_on_rank,
        next2_hired_on,
        days_since_prev_hired_on,
        last_known_hired_on,
        ABS(hired_on_rank - 5) AS hired_on_rank_distance,
        DATE_TRUNC('YEAR', first_known_hired_on) AS first_known_hired_on_year,
        UPPER(TRIM(team_ref)) AS team_ref_clean
    FROM ranked
),
ranked_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on_percent_rank,
        hired_on_percent_rank_2,
        hired_on_rank,
        next2_hired_on,
        last_known_hired_on,
        hired_on_rank_distance,
        RANK() OVER (ORDER BY hired_on_rank) AS hired_on_rank_rank,
        ROUND(hired_on_percent_rank_2 - AVG(hired_on_percent_rank_2) OVER (PARTITION BY DATE_TRUNC('MONTH', last_known_hired_on)), 2) AS hired_on_percent_rank_2_vs_last_known_hired_on_m,
        ROUND(PERCENT_RANK() OVER w_all_by_hired_on_rank, 3) AS hired_on_rank_percent_rank
    FROM derived
    WINDOW w_all_by_hired_on_rank AS (ORDER BY hired_on_rank, id)
)
-- Final projection
SELECT
    id,
    agent_name,
    team,
    tier,
    hired_on_percent_rank,
    hired_on_percent_rank_2,
    hired_on_rank,
    hired_on_percent_rank_2_vs_last_known_hired_on_m,
    hired_on_rank_percent_rank,
    hired_on_percent_rank_2 + hired_on_rank AS hired_on_percent_rank_2_plus_hired_on_rank,
    YEAR(next2_hired_on) AS next2_hired_on_year
FROM ranked_2
WHERE team = 'Onboarding'
ORDER BY agent_name DESC, id
LIMIT 10
