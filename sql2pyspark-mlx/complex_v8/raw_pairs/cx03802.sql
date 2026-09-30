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
derived AS (
    -- Label and score rows
    SELECT
        *,
        premium + claimed_amount AS premium_plus_claimed_amount,
        CASE WHEN premium >= 3719.7 THEN 'A' WHEN premium >= 2033.8 THEN 'B' ELSE 'C' END AS premium_tier,
        DATE_TRUNC('YEAR', claim_date) AS claim_date_year
    FROM in_window
),
unique_rows AS (
    -- Keep the latest row per policy_id, adjuster_id
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        status,
        adjuster_id,
        product,
        premium,
        premium_plus_claimed_amount,
        premium_tier,
        claim_date_year
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_id, adjuster_id ORDER BY claim_date DESC, id DESC) AS rn
        FROM derived
    ) ranked
    WHERE rn = 1
)
-- Subtotals with GROUPING SETS
SELECT
    product,
    status,
    COUNT(DISTINCT status) AS distinct_status
FROM unique_rows
GROUP BY GROUPING SETS ((product, status), (product), (status), ())
ORDER BY product NULLS LAST, status NULLS LAST
