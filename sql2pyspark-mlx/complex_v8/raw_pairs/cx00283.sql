WITH in_window AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.status,
        c.adjuster_id,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        adjuster_id,
        product,
        CONCAT(product, '#', CAST(adjuster_id AS STRING)) AS product_ref,
        DATEDIFF(DATE '2026-01-01', claim_date) AS days_since_claim_date
    FROM in_window
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        product,
        product_ref,
        days_since_claim_date,
        days_since_claim_date - LAG(days_since_claim_date) OVER (PARTITION BY MONTH(claim_date), product ORDER BY days_since_claim_date, id) AS days_since_claim_date_change,
        COUNT(*) OVER (PARTITION BY product) AS rows_in_product,
        SUM(days_since_claim_date) OVER (PARTITION BY MONTH(claim_date), product ORDER BY days_since_claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_since_claim_date
    FROM tagged
)
SELECT
    id,
    claimed_amount,
    days_since_claim_date_change,
    rows_in_product,
    running_days_since_claim_date,
    COALESCE(days_since_claim_date, 0) AS days_since_claim_date_filled
FROM with_windows
ORDER BY rows_in_product DESC NULLS FIRST, claimed_amount DESC NULLS FIRST
