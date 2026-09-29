WITH covered AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.approved_amount,
        c.adjuster_id,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(DATE '2025-06-30', claim_date) AS days_since_claim_date,
        CASE WHEN premium >= 4417.0 THEN 'A' WHEN premium >= 1541.8 THEN 'B' ELSE 'C' END AS premium_tier
    FROM covered
),
latest_per_key AS (
    -- Keep the earliest row per product
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        approved_amount,
        adjuster_id,
        product,
        premium,
        days_since_claim_date,
        premium_tier
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product ORDER BY claim_date, id) AS rn
        FROM enriched
    ) ranked
    WHERE rn = 1
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        claim_date,
        claimed_amount,
        approved_amount,
        adjuster_id,
        product,
        days_since_claim_date,
        premium_tier,
        INITCAP(TRIM(premium_tier)) AS premium_tier_clean,
        DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_label
    FROM latest_per_key
    WHERE policy_id >= 24
)
-- Final projection
SELECT
    id,
    claimed_amount,
    approved_amount,
    adjuster_id,
    product,
    days_since_claim_date,
    premium_tier,
    premium_tier_clean,
    claim_date_label,
    CASE WHEN days_since_claim_date >= 420 THEN 'HIGH' WHEN days_since_claim_date >= 210 THEN 'MEDIUM' ELSE 'LOW' END AS days_since_claim_date_tier,
    COALESCE(product, 'UNKNOWN') AS product_or_unknown
FROM derived
ORDER BY claim_date_label
