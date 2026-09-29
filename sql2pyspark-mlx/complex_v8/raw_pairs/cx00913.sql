WITH scoped AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE claim_date < DATE '2025-04-11'
      AND approved_amount BETWEEN 0.0 AND 33690.0
),
rollup_base AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', reported_at) AS reported_at_month,
        COUNT(*) FILTER (WHERE claim_type = 'flood') AS n_claim_type_flood,
        ROUND(SUM(approved_amount) / NULLIF(SUM(claimed_amount), 0), 3) AS approved_amount_to_claimed_amount_ratio,
        SUM(CASE WHEN status <> 'PAID' THEN 1 ELSE 0 END) AS n_status_paid,
        SUM(claimed_amount) FILTER (WHERE claim_type = 'medical') AS claimed_amount_claim_type_medical,
        ROUND(SUM(approved_amount), 2) AS total_approved_amount
    FROM scoped
    GROUP BY DATE_TRUNC('MONTH', reported_at)
    HAVING MAX(approved_amount) > 0.0
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM rollup_base
    WHERE approved_amount_to_claimed_amount_ratio > (SELECT AVG(approved_amount_to_claimed_amount_ratio) FROM rollup_base)
)
SELECT
    reported_at_month,
    n_claim_type_flood,
    n_status_paid,
    claimed_amount_claim_type_medical,
    total_approved_amount,
    FLOOR(total_approved_amount / 50000) * 50000 AS total_approved_amount_bucket,
    total_approved_amount + n_claim_type_flood AS total_approved_amount_plus_n_claim_type_flood
FROM with_activity
ORDER BY n_claim_type_flood DESC, n_status_paid DESC
