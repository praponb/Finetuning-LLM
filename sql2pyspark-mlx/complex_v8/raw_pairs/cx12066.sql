WITH matched AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.adjuster_id,
        c.claim_type,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
latest_per_key AS (
    -- Keep the latest row per product, adjuster_id
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        adjuster_id,
        claim_type,
        product,
        premium
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product, adjuster_id ORDER BY claim_date DESC, id DESC) AS recency_rank
        FROM matched
    ) ranked
    WHERE recency_rank = 1
)
-- Subtotals with ROLLUP
SELECT
    policy_id,
    adjuster_id,
    COALESCE(product, 'ALL') AS product,
    SUM(CASE WHEN claim_type = 'medical' THEN 1 ELSE 0 END) AS n_claim_type_medical
FROM latest_per_key
GROUP BY ROLLUP(policy_id, adjuster_id, product)
