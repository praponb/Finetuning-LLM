WITH valid_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug,
        ROUND(claimed_amount * 0.25, 2) AS claimed_amount_adj
    FROM claims
    WHERE approved_amount > 9389.3
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MAX(c.reported_at) FROM claims c WHERE c.policy_id = s.policy_id) AS last_claim_reported_at,
        (SELECT COUNT(*) FILTER (WHERE c.claim_type IS NOT NULL) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_claim_type_count
    FROM valid_claims s
)
-- Subtotals with CUBE
SELECT
    status,
    claim_claim_type_count,
    adjuster_id,
    ROUND(SUM(claim_claim_type_count) / NULLIF(SUM(claimed_amount), 0), 3) AS claim_claim_type_count_to_claimed_amount_ratio,
    COUNT(*) AS row_count,
    COUNT(DISTINCT status) AS distinct_status
FROM with_stats
GROUP BY CUBE(status, claim_claim_type_count, adjuster_id)
ORDER BY status NULLS LAST, claim_claim_type_count NULLS LAST, adjuster_id NULLS LAST
