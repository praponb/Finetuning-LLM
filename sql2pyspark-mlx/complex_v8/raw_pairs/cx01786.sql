WITH valid_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at
    FROM claims
    WHERE approved_amount >= 2569.2
      AND claim_type = 'delay'
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.reported_at,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.status AS claim_status,
        c.reported_at AS claim_reported_at,
        c.claim_date AS claim_claim_date
    FROM valid_claims s
    JOIN claims c ON c.policy_id = s.policy_id AND c.approved_amount >= 0.0
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        claim_id,
        claim_reported_at,
        claim_claim_date,
        DATE_TRUNC('HOUR', claim_reported_at) AS claim_reported_at_hour
    FROM detail_rows
    WHERE adjuster_id <= 7
),
rollup_base AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(claim_claim_date, 'yyyy-MM') AS claim_claim_date_ym,
        claim_id,
        DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_ym,
        PERCENTILE_APPROX(claimed_amount, 0.5) AS median_claimed_amount,
        ROUND(100 * COUNT(CASE WHEN claim_type IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_claim_type_is_not_null
    FROM tagged
    WHERE YEAR(claim_claim_date) = 2025
    GROUP BY 1, 2, 3
    HAVING SUM(approved_amount) > 1273.7
)
-- Final projection
SELECT
    claim_claim_date_ym,
    claim_date_ym,
    median_claimed_amount,
    pct_claim_type_is_not_null
FROM rollup_base
ORDER BY pct_claim_type_is_not_null DESC, median_claimed_amount
