WITH filtered_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        status,
        CONCAT(status, '#', CAST(policy_id AS STRING)) AS status_ref
    FROM claims
    WHERE policy_id IS NOT NULL
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        COUNT(*) FILTER (WHERE status NOT IN ('PAID', 'OPEN')) AS claim_status_count,
        MAX(approved_amount) AS max_claim_approved_amount
    FROM claims
    GROUP BY policy_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_status_count, 0) AS claim_status_count,
        c.max_claim_approved_amount
    FROM filtered_claims s
    LEFT JOIN claim_stats c ON c.policy_id = s.policy_id
),
grouped AS (
    -- Summarise per group
    SELECT
        adjuster_id,
        policy_id,
        SUM(claim_status_count) AS total_claim_status_count,
        COUNT(DISTINCT adjuster_id) AS distinct_adjuster_ids
    FROM with_counts
    WHERE status IN ('PAID', 'APPROVED')
    GROUP BY adjuster_id, policy_id
    HAVING SUM(max_claim_approved_amount) > 12500.0
)
-- Final projection
SELECT
    adjuster_id,
    distinct_adjuster_ids,
    ABS(total_claim_status_count - 31) AS total_claim_status_count_distance
FROM grouped
ORDER BY distinct_adjuster_ids NULLS LAST
