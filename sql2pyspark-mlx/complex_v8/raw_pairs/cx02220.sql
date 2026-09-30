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
        reported_at,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM claims
    WHERE status = 'REJECTED'
      AND claim_date >= DATE '2024-08-01'
),
without_matches AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM scoped s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
        )
      AND s.claimed_amount > 12010.5
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        ROUND(AVG(approved_amount), 2) AS avg_claim_approved_amount,
        COUNT(*) AS claim_count,
        MIN(claim_date) AS first_claim_date
    FROM claims
    GROUP BY policy_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.avg_claim_approved_amount,
        COALESCE(c.claim_count, 0) AS claim_count,
        c.first_claim_date
    FROM without_matches s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type,
    claimed_amount,
    approved_amount,
    status,
    reported_at,
    status_slug,
    avg_claim_approved_amount,
    claim_count,
    approved_amount - claim_count AS approved_amount_minus_claim_count
FROM with_stats
