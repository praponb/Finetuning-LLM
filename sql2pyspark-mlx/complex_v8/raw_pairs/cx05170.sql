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
        status,
        reported_at,
        SUBSTRING(TRIM(status), 1, 4) AS status_prefix,
        approved_amount - claimed_amount AS approved_amount_minus_claimed_amount,
        CASE WHEN approved_amount >= 22250.6 THEN 'large' WHEN approved_amount >= 4554.8 THEN 'medium' ELSE 'small' END AS approved_amount_tier
    FROM claims
    WHERE claim_type = 'flood'
      AND status = 'APPROVED'
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE c.claim_type IS NOT NULL) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_claim_type_count,
        (SELECT COUNT(CASE WHEN c.claim_type IN ('delay', 'flood') THEN 1 END) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_claim_type_count_2,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count
    FROM prepared s
),
latest_per_key AS (
    -- Keep the latest row per claim_type
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
        status_prefix,
        approved_amount_minus_claimed_amount,
        approved_amount_tier,
        claim_claim_type_count,
        claim_claim_type_count_2,
        claim_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_type ORDER BY claim_claim_type_count_2 DESC, id DESC) AS row_num
        FROM with_stats
    ) ranked
    WHERE row_num = 1
),
detail_rows_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.approved_amount_minus_claimed_amount,
        s.approved_amount_tier,
        s.claim_claim_type_count,
        s.claim_count,
        c.id AS claim_id,
        c.claim_date AS claim_claim_date,
        c.status AS claim_status,
        c.reported_at AS claim_reported_at
    FROM latest_per_key s
    JOIN claims c ON c.policy_id = s.policy_id AND c.claim_date < DATE '2025-01-04'
)
SELECT id, claim_id, measure, reading
FROM (
    SELECT id, claim_id, claim_claim_type_count, claim_count
    FROM detail_rows_claims
) src
UNPIVOT (
    reading FOR measure IN (claim_claim_type_count, claim_count)
) unp
