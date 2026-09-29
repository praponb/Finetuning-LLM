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
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.claim_date,
        s.adjuster_id,
        s.product,
        s.premium,
        c.id AS claim_id,
        c.status AS claim_status,
        c.adjuster_id AS claim_adjuster_id,
        c.claim_type AS claim_claim_type,
        c.reported_at AS claim_reported_at
    FROM covered s
    JOIN claims c ON c.policy_id = s.policy_id AND c.claim_type = 'medical'
),
per_group AS (
    -- Summarise per group
    SELECT
        product,
        DATE_TRUNC('MONTH', claim_date) AS claim_date_month,
        claim_claim_type,
        ROUND(100 * COUNT(CASE WHEN claim_status <> 'REJECTED' THEN 1 END) / COUNT(*), 1) AS pct_claim_status_rejected,
        COUNT(*) AS row_count,
        SUM(premium) AS total_premium,
        COUNT(*) FILTER (WHERE claim_claim_type IS NOT NULL) AS n_claim_claim_type_is_not_null,
        COUNT(DISTINCT claim_claim_type) AS distinct_claim_claim_types
    FROM exploded_detail_claims
    WHERE claim_status IN ('REJECTED', 'OPEN', 'APPROVED')
    GROUP BY product, DATE_TRUNC('MONTH', claim_date), claim_claim_type
    HAVING COUNT(*) >= 2
)
-- Native SQL UNPIVOT (wide to long)
SELECT product, claim_date_month, field, value
FROM (
    SELECT product, claim_date_month, distinct_claim_claim_types, n_claim_claim_type_is_not_null
    FROM per_group
) src
UNPIVOT (
    value FOR field IN (distinct_claim_claim_types, n_claim_claim_type_is_not_null)
) unp
