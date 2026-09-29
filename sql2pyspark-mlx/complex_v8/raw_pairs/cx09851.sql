WITH prepared AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        tags
    FROM tickets
    WHERE HOUR(closed_at) BETWEEN 6 AND 8
),
enriched_agent AS (
    -- Enrich with dimension data
    SELECT s.*, su.tier AS agent_tier, su.team AS agent_team
    FROM prepared s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.team = 'Billing'
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(LAG(opened_at) OVER (PARTITION BY TO_DATE(closed_at) ORDER BY opened_at, id))) / 60, 1) AS minutes_since_prev_opened_at,
        NTILE(5) OVER (PARTITION BY TO_DATE(closed_at) ORDER BY opened_at, id) AS opened_at_quintile,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(LAG(opened_at) OVER (PARTITION BY agent_team, agent_id ORDER BY closed_at, id))) / 60, 1) AS minutes_since_prev_opened_at_2,
        LAST_VALUE(closed_at) IGNORE NULLS OVER (PARTITION BY agent_id, agent_tier ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_closed_at,
        RANK() OVER w_all_by_opened_at_ties AS opened_at_rank,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY agent_id ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        ROUND(CUME_DIST() OVER (PARTITION BY TO_DATE(closed_at) ORDER BY opened_at), 3) AS opened_at_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY agent_team, agent_id ORDER BY closed_at, id), 3) AS closed_at_percent_rank,
        DENSE_RANK() OVER (PARTITION BY agent_team, agent_id ORDER BY closed_at) AS closed_at_dense_rank,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(LAG(closed_at) OVER (PARTITION BY agent_team, agent_id ORDER BY closed_at, id))) / 60, 1) AS minutes_since_prev_closed_at,
        NTILE(3) OVER (PARTITION BY agent_team, agent_id ORDER BY closed_at, id) AS closed_at_tercile,
        NTILE(3) OVER w_closed_at_to_date_by_opened AS opened_at_tercile
    FROM enriched_agent
    WINDOW w_all_by_opened_at_ties AS (ORDER BY opened_at DESC), w_closed_at_to_date_by_opened AS (PARTITION BY TO_DATE(closed_at) ORDER BY opened_at, id)
),
first_rows AS (
    -- Top 5 per group
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        tags,
        agent_tier,
        agent_team,
        minutes_since_prev_opened_at,
        opened_at_quintile,
        minutes_since_prev_opened_at_2,
        last_known_closed_at,
        opened_at_rank,
        second_closed_at,
        opened_at_cume_dist,
        closed_at_percent_rank,
        minutes_since_prev_closed_at,
        closed_at_tercile,
        opened_at_tercile
    FROM sequenced
    WHERE closed_at_dense_rank <= 5
)
-- Final projection
SELECT
    id,
    tags,
    agent_tier,
    opened_at_quintile,
    minutes_since_prev_opened_at_2,
    last_known_closed_at,
    opened_at_rank,
    second_closed_at,
    opened_at_cume_dist,
    closed_at_percent_rank,
    opened_at_tercile,
    INITCAP(TRIM(agent_team)) AS agent_team_clean
FROM first_rows
ORDER BY minutes_since_prev_opened_at_2 DESC, opened_at_cume_dist
