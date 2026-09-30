WITH filtered_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        ROUND(claimed_amount * 0.9, 2) AS claimed_amount_adj,
        CASE WHEN approved_amount >= 26237.1 THEN 'HIGH' WHEN approved_amount >= 1364.5 THEN 'MEDIUM' ELSE 'LOW' END AS approved_amount_tier
    FROM claims
    WHERE approved_amount <= 32878.5
      AND adjuster_id IS NOT NULL
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claimed_amount,
        s.status,
        s.claimed_amount_adj,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.reported_at AS claim_reported_at,
        c.claimed_amount AS claim_claimed_amount,
        c.claim_date AS claim_claim_date
    FROM filtered_claims s
    LEFT JOIN claims c ON c.policy_id = s.policy_id AND c.claim_date < DATE '2025-01-04'
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(claimed_amount) OVER (PARTITION BY status ORDER BY claimed_amount_adj DESC, id, claim_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS claimed_amount_sum_3_prior,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY policy_id), 2) AS policy_id_avg_claimed_amount,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY claimed_amount_adj DESC, id, claim_id) AS rn,
        DATEDIFF(claim_claim_date, LAG(claim_claim_date) OVER (PARTITION BY status ORDER BY claimed_amount_adj DESC, id, claim_id)) AS days_since_prev_claim_claim_date
    FROM with_children
),
summary AS (
    -- Group-level totals
    SELECT
        claim_id,
        DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_ym,
        SUM(claim_claimed_amount) AS total_claim_claimed_amount,
        COUNT(*) AS cnt
    FROM ranked
    GROUP BY 1, 2
),
with_details_claim AS (
    -- Join claims
    SELECT
        s.*,
        c.status AS claim_status,
        c.reported_at AS claim_reported_at,
        c.claim_date
    FROM summary s
    LEFT JOIN claims c ON c.id = s.claim_id AND c.claim_type IS NOT NULL
)
-- Final projection
SELECT
    claim_id,
    claim_date_ym,
    total_claim_claimed_amount,
    cnt,
    claim_date,
    INITCAP(TRIM(claim_status)) AS claim_status_clean
FROM with_details_claim
ORDER BY claim_id DESC
