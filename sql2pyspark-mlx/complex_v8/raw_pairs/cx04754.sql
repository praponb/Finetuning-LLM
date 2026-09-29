WITH scoped_holders AS (
    -- Filter policyholders
    SELECT
        id,
        holder_name,
        region,
        joined_on,
        segment
    FROM policyholders
    WHERE risk_score > 0.1
),
line_level AS (
    -- One row per policy
    SELECT
        s.id,
        s.holder_name,
        s.joined_on,
        p.id AS policy_id,
        p.premium AS policy_premium,
        p.product AS policy_product,
        p.end_date AS policy_end_date
    FROM scoped_holders s
    JOIN policies p ON p.holder_id = s.id
),
latest_per_key AS (
    -- Keep the earliest row per policy_product
    SELECT
        id,
        holder_name,
        joined_on,
        policy_id,
        policy_premium,
        policy_product,
        policy_end_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_product ORDER BY joined_on, id, policy_id) AS rn
        FROM line_level
    ) ranked
    WHERE rn = 1
)
SELECT
    policy_id,
    policy_product,
    GROUPING_ID() AS grouping_level,
    ROUND(STDDEV(policy_premium), 2) AS stddev_policy_premium
FROM latest_per_key
GROUP BY CUBE(policy_id, policy_product)
ORDER BY policy_id NULLS LAST, policy_product NULLS LAST
