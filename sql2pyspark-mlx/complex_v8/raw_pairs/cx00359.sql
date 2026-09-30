WITH in_window AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.claim_type,
        c.approved_amount,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        claim_type,
        approved_amount,
        product,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY product)) AS distinct_claim_type_in_product,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY product), 0), 4) AS premium_share_of_product
    FROM in_window
)
-- Final projection
SELECT
    id,
    claim_type,
    product,
    distinct_claim_type_in_product,
    COALESCE(premium_share_of_product, 0.0) AS premium_share_of_product_filled,
    ABS(approved_amount - 14595.6) AS approved_amount_distance
FROM sequenced
ORDER BY distinct_claim_type_in_product DESC NULLS FIRST, product DESC NULLS FIRST, id
LIMIT 10
