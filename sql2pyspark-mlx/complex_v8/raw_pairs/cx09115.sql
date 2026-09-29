WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        status,
        reported_at,
        CASE WHEN status IN ('OPEN', 'APPROVED') THEN 'group_a' WHEN status = 'REJECTED' THEN 'group_b' ELSE 'other' END AS status_group,
        DATEDIFF(DATE '2025-06-30', claim_date) AS days_since_claim_date,
        ROUND(claimed_amount / NULLIF(approved_amount, 0), 3) AS claimed_amount_per_approved_amount
    FROM claims
    WHERE adjuster_id IS NOT NULL
      AND reported_at >= TIMESTAMP '2025-01-25 11:00:00'
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        days_since_claim_date,
        claimed_amount_per_approved_amount,
        COALESCE(days_since_claim_date, 0) AS days_since_claim_date_filled,
        ROUND(days_since_claim_date / NULLIF(approved_amount, 0), 3) AS days_since_claim_date_per_approved_amount,
        ROUND(approved_amount / NULLIF(days_since_claim_date, 0), 3) AS approved_amount_per_days_since_claim_date
    FROM base
    WHERE approved_amount <= 20843.1
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.days_since_claim_date,
        s.days_since_claim_date_filled,
        s.approved_amount_per_days_since_claim_date,
        c.id AS claim_id,
        c.claim_date,
        c.status AS claim_status,
        c.reported_at AS claim_reported_at
    FROM scored s
    LEFT JOIN claims c ON c.adjuster_id = s.adjuster_id
)
-- Subtotals with ROLLUP
SELECT
    adjuster_id,
    claim_status,
    MIN(claim_date) AS first_claim_date,
    MIN(days_since_claim_date_filled) AS min_days_since_claim_date_filled
FROM exploded_detail_claims
GROUP BY ROLLUP(adjuster_id, claim_status)
ORDER BY adjuster_id NULLS LAST, claim_status NULLS LAST
