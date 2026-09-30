WITH matched AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.adjuster_id,
        c.approved_amount,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(premium / NULLIF(approved_amount, 0), 3) AS premium_per_approved_amount,
        CASE WHEN product IN ('HEALTH', 'HOME') THEN 'group_a' WHEN product = 'LIFE' THEN 'group_b' ELSE 'other' END AS product_group,
        ROUND(approved_amount / NULLIF(claimed_amount, 0), 3) AS approved_amount_per_claimed_amount
    FROM matched
    WHERE policy_id >= 19
),
without_matches AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM labeled s
    WHERE s.adjuster_id IN (
            SELECT c.adjuster_id
            FROM claims c
            WHERE c.claimed_amount <= 33975.8
              AND c.status <> 'OPEN'
        )
      AND s.product_group NOT IN ('group_b', 'other')
),
enriched_policy AS (
    -- Join policies
    SELECT
        s.*,
        p.start_date AS policy_start_date,
        p.status AS policy_status,
        p.product AS policy_product
    FROM without_matches s
    JOIN policies p ON p.id = s.policy_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT product_group, adjuster_id, policy_product, approved_amount_per_claimed_amount
    FROM enriched_policy
) src
PIVOT (
    MIN(approved_amount_per_claimed_amount)
    FOR policy_product IN ('TRAVEL', 'AUTO', 'HEALTH', 'HOME')
)
ORDER BY product_group, adjuster_id
