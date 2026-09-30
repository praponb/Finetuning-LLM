WITH filtered_agents AS (
    -- Filter support_agents
    SELECT
        id,
        agent_name,
        tier,
        hired_on,
        SUBSTRING(TRIM(agent_name), 1, 2) AS agent_name_prefix
    FROM support_agents
    WHERE hired_on BETWEEN DATE '2024-05-02' AND DATE '2025-03-30'
      AND tier IS NOT NULL
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(hired_on) IGNORE NULLS OVER (ORDER BY hired_on NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_hired_on,
        ROW_NUMBER() OVER (ORDER BY hired_on NULLS LAST, id) AS rn,
        ROW_NUMBER() OVER w_tier_by_hired_on AS seq_no
    FROM filtered_agents
    WINDOW w_tier_by_hired_on AS (PARTITION BY tier ORDER BY hired_on, id)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(rn) OVER w_tier AS tier_max_rn,
        ROUND(seq_no - AVG(seq_no) OVER (PARTITION BY tier), 2) AS seq_no_vs_tier_avg,
        ROUND(CUME_DIST() OVER (ORDER BY hired_on), 3) AS hired_on_cume_dist,
        MAX(seq_no) OVER (PARTITION BY tier, seq_no ORDER BY seq_no ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_seq_no_so_far
    FROM with_windows
    WINDOW w_tier AS (PARTITION BY tier)
)
-- Final projection
SELECT
    id,
    tier,
    hired_on,
    agent_name_prefix,
    rn,
    seq_no,
    tier_max_rn,
    seq_no_vs_tier_avg,
    hired_on_cume_dist,
    max_seq_no_so_far,
    CASE WHEN tier IN ('T2', 'T1') THEN 'group_a' WHEN tier = 'T3' THEN 'group_b' ELSE 'other' END AS tier_group
FROM windowed
ORDER BY hired_on_cume_dist DESC, id
LIMIT 5
