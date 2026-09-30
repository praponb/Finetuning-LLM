WITH prepared AS (
    -- Filter support_agents
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        LOWER(REGEXP_REPLACE(TRIM(tier), ' ', '_')) AS tier_slug,
        CONCAT(team, '#', CAST(id AS STRING)) AS team_ref
    FROM support_agents
    WHERE LOWER(agent_name) LIKE '%h %'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        agent_name,
        tier,
        hired_on,
        tier_slug,
        team_ref,
        NTH_VALUE(hired_on, 2) OVER (PARTITION BY YEAR(hired_on), tier ORDER BY hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_hired_on,
        LAST_VALUE(hired_on) IGNORE NULLS OVER (PARTITION BY team ORDER BY hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_hired_on,
        LAST_VALUE(hired_on) IGNORE NULLS OVER (PARTITION BY YEAR(hired_on), tier ORDER BY hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_hired_on_2,
        NTH_VALUE(hired_on, 2) OVER (PARTITION BY tier ORDER BY hired_on DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_hired_on_2,
        NTH_VALUE(hired_on, 2) OVER (ORDER BY hired_on ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_hired_on_3,
        FIRST_VALUE(hired_on) OVER (PARTITION BY team ORDER BY hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_hired_on,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', hired_on) ORDER BY hired_on ASC, id) AS rn,
        DATEDIFF(hired_on, LAG(hired_on) OVER (PARTITION BY tier ORDER BY hired_on DESC NULLS FIRST, id)) AS days_since_prev_hired_on,
        ROW_NUMBER() OVER (PARTITION BY YEAR(hired_on), tier ORDER BY hired_on DESC, id) AS hired_on_seq,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', hired_on) ORDER BY hired_on ASC) AS hired_on_dense_rank
    FROM prepared
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(first_hired_on, 'yyyy-MM') AS first_hired_on_ym,
        YEAR(last_known_hired_on_2) AS last_known_hired_on_2_year,
        COUNT(*) FILTER (WHERE tier NOT IN ('T3', 'T1')) AS n_tier_not_in_t3_t1,
        MAX(last_known_hired_on) AS last_last_known_hired_on,
        MIN(rn) AS min_rn
    FROM ranked
    GROUP BY DATE_FORMAT(first_hired_on, 'yyyy-MM'), YEAR(last_known_hired_on_2)
    HAVING AVG(rn) > 3
)
SELECT
    first_hired_on_ym,
    last_known_hired_on_2_year,
    n_tier_not_in_t3_t1,
    last_last_known_hired_on,
    min_rn,
    LENGTH(TRIM(first_hired_on_ym)) AS first_hired_on_ym_len
FROM per_group
ORDER BY first_hired_on_ym, first_hired_on_ym_len
