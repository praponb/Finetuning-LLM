WITH cleaned AS (
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
        CONCAT_WS(' / ', status, claim_type) AS status_claim_type_label
    FROM claims
    WHERE claim_type IN ('collision', 'fire')
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count,
        (SELECT MIN(c.reported_at) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS first_claim_reported_at,
        (SELECT MAX(c.reported_at) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS last_claim_reported_at
    FROM cleaned s
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(status, 'ALL') AS status,
    COALESCE(claim_type, 'ALL') AS claim_type,
    GROUPING_ID() AS grouping_level,
    SUM(claim_count) AS total_claim_count
FROM with_counts
GROUP BY GROUPING SETS ((status, claim_type), (status))
ORDER BY status NULLS LAST, claim_type NULLS LAST
