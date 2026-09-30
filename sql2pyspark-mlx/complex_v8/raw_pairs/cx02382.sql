WITH base_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        DATE_FORMAT(claim_date, 'yyyy-MM-dd') AS claim_date_label,
        FLOOR(approved_amount / 10000) * 10000 AS approved_amount_bucket,
        CASE WHEN claimed_amount >= 29336.0 THEN 'HIGH' WHEN claimed_amount >= 17759.3 THEN 'MEDIUM' ELSE 'LOW' END AS claimed_amount_tier
    FROM claims
    WHERE status IS NOT NULL
      AND claimed_amount < 29191.2
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count,
        (SELECT COUNT(*) FILTER (WHERE c.claim_type = 'delay') FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_claim_type_count
    FROM base_claims s
),
scored AS (
    -- Derive helper columns
    SELECT *, COALESCE(claim_count, 0) AS claim_count_filled
    FROM with_stats
    WHERE claim_date_label IS NOT NULL
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claim_type,
    status,
    approved_amount_bucket,
    claimed_amount_tier,
    claim_count,
    claim_count_filled
FROM scored
WHERE status = 'IN_REVIEW'
