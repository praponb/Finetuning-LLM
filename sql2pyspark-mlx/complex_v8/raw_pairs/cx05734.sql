WITH scoped_claims AS (
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
        COALESCE(approved_amount, 0.0) AS approved_amount_filled,
        approved_amount - claimed_amount AS approved_amount_minus_claimed_amount,
        CONCAT_WS(': ', claim_type, status) AS claim_type_status_label
    FROM claims
),
rollup_base AS (
    -- Group-level totals
    SELECT
        status,
        adjuster_id,
        DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_ym,
        ROUND(SUM(approved_amount_minus_claimed_amount) / NULLIF(SUM(approved_amount_filled), 0), 3) AS approved_amount_minus_claimed_amount_to_approved,
        MAX(claimed_amount) AS max_claimed_amount,
        MAX(claim_date) AS last_claim_date,
        MAX(approved_amount_minus_claimed_amount) AS max_approved_amount_minus_claimed_amount,
        MAX(approved_amount) AS max_approved_amount
    FROM scoped_claims
    GROUP BY status, adjuster_id, DATE_FORMAT(claim_date, 'yyyy-MM')
    HAVING AVG(approved_amount_filled) > 5000.0
),
qualified AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM rollup_base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.reported_at >= TIMESTAMP '2025-01-19 14:00:00'
        )
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT
        adjuster_id,
        COUNT(CASE WHEN claim_type IS NOT NULL THEN 1 END) AS claim_claim_type_count
    FROM claims
    GROUP BY adjuster_id
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_claim_type_count, 0) AS claim_claim_type_count
    FROM qualified s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
)
SELECT
    status,
    claim_date_ym,
    approved_amount_minus_claimed_amount_to_approved,
    last_claim_date,
    max_approved_amount_minus_claimed_amount,
    max_claimed_amount - claim_claim_type_count AS max_claimed_amount_minus_claim_claim_type_count
FROM profiled
ORDER BY status, approved_amount_minus_claimed_amount_to_approved
