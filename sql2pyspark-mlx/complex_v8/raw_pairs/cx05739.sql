WITH prepared AS (
    -- Filter policyholders
    SELECT
        id,
        risk_score,
        joined_on,
        segment,
        LOWER(REGEXP_REPLACE(TRIM(holder_name), ' ', '_')) AS holder_name_slug
    FROM policyholders
    WHERE joined_on >= DATE '2024-03-22'
),
exploded_detail_policys AS (
    -- One row per policy
    SELECT
        s.id,
        s.joined_on,
        p.id AS policy_id,
        p.product AS policy_product,
        p.premium AS policy_premium,
        p.start_date AS policy_start_date
    FROM prepared s
    LEFT JOIN policies p ON p.holder_id = s.id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        joined_on,
        policy_id,
        policy_product,
        policy_premium,
        policy_start_date,
        policy_premium - LAG(policy_premium) OVER w_all_by_joined_on AS policy_premium_change,
        ROUND(PERCENT_RANK() OVER (ORDER BY joined_on, id, policy_id), 3) AS joined_on_percent_rank,
        ROUND(policy_premium / NULLIF(SUM(policy_premium) OVER (PARTITION BY policy_id, policy_product), 0), 4) AS policy_premium_share_of_policy_id_policy_product,
        MAX(policy_premium) OVER w_policy_id_policy_product AS policy_id_policy_product_max_policy_premium
    FROM exploded_detail_policys
    WINDOW w_all_by_joined_on AS (ORDER BY joined_on, id, policy_id), w_policy_id_policy_product AS (PARTITION BY policy_id, policy_product)
),
without_matches AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.id
              AND p.status IS NOT NULL
              AND p.start_date < DATE '2025-05-30'
        )
      AND s.joined_on_percent_rank BETWEEN 0.1 AND 0.7
)
SELECT *
FROM (
    SELECT joined_on, policy_start_date, policy_product, policy_id_policy_product_max_policy_premium
    FROM without_matches
) src
PIVOT (
    COUNT(policy_id_policy_product_max_policy_premium)
    FOR policy_product IN ('HEALTH', 'HOME')
)
