WITH prepared AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket,
        approved_amount + claimed_amount AS approved_amount_plus_claimed_amount
    FROM claims
    WHERE policy_id IS NOT NULL
      AND claimed_amount > 12010.5
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.reported_at) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS first_claim_reported_at,
        (SELECT COUNT(CASE WHEN c.status IN ('APPROVED', 'IN_REVIEW', 'PAID') THEN 1 END) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_status_count,
        (SELECT MIN(c.claimed_amount) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS min_claim_claimed_amount
    FROM prepared s
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, value
FROM (
    SELECT id, claimed_amount_bucket, claim_status_count
    FROM with_stats
) src
UNPIVOT (
    value FOR field IN (claimed_amount_bucket, claim_status_count)
) unp
