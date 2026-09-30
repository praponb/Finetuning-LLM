WITH valid AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        region,
        segment
    FROM policyholders
    WHERE region <> 'South'
      AND LOWER(holder_name) LIKE '%nt%'
),
line_level AS (
    -- One row per policy
    SELECT
        s.id,
        s.segment,
        p.id AS policy_id,
        p.premium AS policy_premium,
        p.start_date AS policy_start_date
    FROM valid s
    JOIN policies p ON p.holder_id = s.id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(policy_premium) IGNORE NULLS OVER (PARTITION BY policy_id, segment ORDER BY policy_premium DESC, id, policy_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_policy_premium,
        DENSE_RANK() OVER (PARTITION BY policy_id, segment ORDER BY policy_premium DESC, id, policy_id) AS policy_premium_dense_rank,
        NTILE(10) OVER (PARTITION BY policy_id, segment ORDER BY policy_premium DESC, id, policy_id) AS policy_premium_decile,
        LAG(policy_start_date) OVER (PARTITION BY policy_id, segment ORDER BY policy_start_date DESC NULLS FIRST, id, policy_id) AS prev_policy_start_date
    FROM line_level
)
SELECT
    policy_id,
    policy_premium_decile,
    policy_premium_dense_rank,
    GROUPING_ID() AS grouping_level,
    ROUND(STDDEV(policy_premium_decile), 2) AS stddev_policy_premium_decile,
    ROUND(SUM(policy_premium_decile) / NULLIF(SUM(policy_premium), 0), 3) AS policy_premium_decile_to_policy_premium_ratio,
    ARRAY_SORT(COLLECT_SET(segment)) AS segment_list
FROM sequenced
GROUP BY GROUPING SETS ((policy_id, policy_premium_decile, policy_premium_dense_rank), (policy_id), ())
