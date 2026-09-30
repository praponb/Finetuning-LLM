WITH matched AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.claim_type,
        c.adjuster_id,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
deduped AS (
    -- Keep the latest row per claim_type
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        claim_type,
        adjuster_id,
        product,
        premium
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_type ORDER BY premium DESC, id DESC) AS row_num
        FROM matched
    ) ranked
    WHERE row_num = 1
),
totals AS (
    -- Summarise per group
    SELECT
        adjuster_id,
        product,
        claim_type,
        SUM(CASE WHEN product NOT IN ('AUTO', 'TRAVEL') THEN 1 ELSE 0 END) AS n_product_not_in_auto_travel,
        ROUND(SUM(claimed_amount) / NULLIF(SUM(premium), 0), 3) AS claimed_amount_to_premium_ratio,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
        COUNT(DISTINCT policy_id) AS distinct_policy_ids,
        MIN(premium) AS min_premium
    FROM deduped
    GROUP BY adjuster_id, product, claim_type
    HAVING AVG(claimed_amount) > 7204.1
)
-- Final projection
SELECT
    adjuster_id,
    claim_type,
    n_product_not_in_auto_travel,
    claimed_amount_to_premium_ratio,
    claim_type_list,
    min_premium,
    COALESCE(min_premium, 0.0) AS min_premium_filled
FROM totals
WHERE SIZE(claim_type_list) >= 2
ORDER BY min_premium DESC
