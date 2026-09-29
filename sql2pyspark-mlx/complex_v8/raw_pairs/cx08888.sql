WITH recent_agents AS (
    -- Filter support_agents
    SELECT
        id,
        agent_name,
        team,
        hired_on
    FROM support_agents
    WHERE agent_name IS NOT NULL
      AND hired_on >= DATE '2024-05-02'
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        team,
        hired_on,
        NTH_VALUE(hired_on, 2) OVER (ORDER BY hired_on DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_hired_on,
        FIRST_VALUE(hired_on) OVER (ORDER BY hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_hired_on,
        DATEDIFF(hired_on, LAG(hired_on) OVER (ORDER BY hired_on DESC NULLS FIRST, id)) AS days_since_prev_hired_on,
        ROUND(CUME_DIST() OVER w_all_by_hired_on_ties, 3) AS hired_on_cume_dist,
        LAG(hired_on) OVER w_all_by_hired_on_2 AS prev_hired_on,
        LEAD(hired_on, 3) OVER (PARTITION BY team ORDER BY hired_on DESC, id) AS next3_hired_on,
        NTILE(5) OVER (ORDER BY hired_on, id) AS hired_on_quintile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY team ORDER BY hired_on DESC, id), 3) AS hired_on_percent_rank,
        ROUND(CUME_DIST() OVER (ORDER BY hired_on DESC NULLS FIRST), 3) AS hired_on_cume_dist_2,
        ROW_NUMBER() OVER (ORDER BY hired_on DESC NULLS FIRST, id) AS hired_on_seq,
        FIRST_VALUE(hired_on) IGNORE NULLS OVER (ORDER BY hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_hired_on,
        DENSE_RANK() OVER w_all_by_hired_on_ties_3 AS hired_on_dense_rank
    FROM recent_agents
    WINDOW w_all_by_hired_on_ties AS (ORDER BY hired_on DESC), w_all_by_hired_on_2 AS (ORDER BY hired_on DESC, id), w_all_by_hired_on_ties_3 AS (ORDER BY hired_on)
),
ranked_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(hired_on_cume_dist) IGNORE NULLS OVER (PARTITION BY hired_on_quintile ORDER BY hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_hired_on_cume_dist,
        ROUND(CUME_DIST() OVER w_hired_on_quintile_by_hired_o_2, 3) AS hired_on_cume_dist_3,
        COUNT(*) OVER w_prev_hired_on_year AS rows_in_prev_hired_on_year,
        NTH_VALUE(next3_hired_on, 2) OVER (PARTITION BY hired_on_quintile ORDER BY prev_hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_next3_hired_on
    FROM ranked
    WINDOW w_hired_on_quintile_by_hired_o_2 AS (PARTITION BY hired_on_quintile ORDER BY hired_on), w_prev_hired_on_year AS (PARTITION BY YEAR(prev_hired_on))
)
-- Final projection
SELECT
    id,
    team,
    hired_on,
    second_hired_on,
    days_since_prev_hired_on,
    hired_on_cume_dist,
    prev_hired_on,
    hired_on_quintile,
    hired_on_percent_rank,
    hired_on_cume_dist_2,
    hired_on_seq,
    hired_on_dense_rank,
    hired_on_cume_dist_3,
    rows_in_prev_hired_on_year,
    CONCAT(team, '#', CAST(days_since_prev_hired_on AS STRING)) AS team_ref,
    GREATEST(hired_on_dense_rank, hired_on_quintile) AS greatest_hired_on_dense_rank_hired_on_quintile
FROM ranked_2
WHERE rows_in_prev_hired_on_year >= 3
