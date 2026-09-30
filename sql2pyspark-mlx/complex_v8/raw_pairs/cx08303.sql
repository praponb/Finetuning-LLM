WITH covered AS (
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
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
without_matches AS (
    -- Keep rows whose policy matches
    SELECT s.*
    FROM covered s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.id = s.policy_id
              AND YEAR(p.end_date) = 2026
              AND p.premium <= 4417.0
        )
),
filtered AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM without_matches s
    WHERE s.adjuster_id IN (
            SELECT c.adjuster_id
            FROM claims c
        )
      AND s.premium < 4678.8
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(claimed_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date) ORDER BY claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claimed_amount,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), product) AS rows_in_claim_date_month_product,
        MIN(claimed_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), product) AS claim_date_month_product_min_claimed_amount
    FROM filtered
)
SELECT
    id,
    policy_id,
    claim_date,
    claimed_amount,
    adjuster_id,
    product,
    last_claimed_amount,
    rows_in_claim_date_month_product,
    claim_date_month_product_min_claimed_amount,
    DATE_TRUNC('YEAR', claim_date) AS claim_date_year
FROM ranked
ORDER BY claim_date_month_product_min_claimed_amount, last_claimed_amount DESC
