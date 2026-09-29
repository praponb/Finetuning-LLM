WITH covered AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.status,
        c.claim_type,
        p.product,
        p.premium
    FROM claims c
    JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY claim_type ORDER BY premium, id), 3) AS premium_percent_rank,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER w_claim_date_month_claim_type, 0), 4) AS claimed_amount_share_of_claim_date_month_claim_t,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY MONTH(claim_date), claim_type), 2) AS claimed_amount_vs_claim_date_month_claim_type_av
    FROM covered
    WINDOW w_claim_date_month_claim_type AS (PARTITION BY MONTH(claim_date), claim_type)
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    status,
    claim_type,
    product,
    premium_percent_rank,
    claimed_amount_share_of_claim_date_month_claim_t,
    claimed_amount_vs_claim_date_month_claim_type_av,
    DATE_TRUNC('WEEK', claim_date) AS claim_date_week
FROM with_windows
WHERE product IN ('LIFE', 'TRAVEL', 'HOME')
ORDER BY status
