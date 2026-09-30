WITH valid AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        status
    FROM claims
    WHERE adjuster_id IS NOT NULL
      AND policy_id IS NOT NULL
),
joined_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        p.product AS policy_product,
        p.holder_id AS policy_holder_id,
        p.start_date AS policy_start_date,
        a.level AS adjuster_level,
        a.adjuster_name
    FROM valid s
    LEFT JOIN policies p ON p.id = s.policy_id AND p.product = 'HOME'
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(policy_product, 'ALL') AS policy_product,
    policy_id,
    GROUPING(policy_product) AS is_total_policy_product,
    SUM(CASE WHEN LOWER(adjuster_name) LIKE '%an%' THEN adjuster_level ELSE 0 END) AS adjuster_level_lower_adjuster_name_like_an
FROM joined_policy
GROUP BY GROUPING SETS ((policy_product, policy_id), (policy_product), ())
ORDER BY policy_product NULLS LAST, policy_id NULLS LAST
