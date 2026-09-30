WITH recent AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        DATE_FORMAT(reported_at, 'yyyy-MM-dd HH:00') AS reported_at_label
    FROM claims
    WHERE approved_amount BETWEEN 0.0 AND 39988.0
),
without_matches AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM recent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.approved_amount <= 32878.5
              AND c.claim_type IN ('flood', 'theft', 'collision')
        )
      AND s.claim_date IS NOT NULL
),
with_children_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.reported_at AS claim_reported_at,
        c.claimed_amount AS claim_claimed_amount,
        c.status AS claim_status
    FROM without_matches s
    LEFT JOIN claims c ON c.adjuster_id = s.adjuster_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN c.claim_type = 'flood' THEN 1 END) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_claim_type_count,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count
    FROM with_children_claims s
),
without_matches_2 AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM summarized_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.approved_amount < 23120.7
        )
      AND s.claim_type NOT IN ('flood', 'medical')
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(approved_amount * 1.1, 2) AS approved_amount_adj,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown,
        LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug
    FROM without_matches_2
    WHERE claim_approved_amount < 22250.6
)
-- Final projection
SELECT
    id,
    claimed_amount,
    approved_amount,
    claim_approved_amount,
    claim_reported_at,
    claim_claimed_amount,
    claim_status,
    claim_type_or_unknown,
    claim_type_slug,
    DATE_TRUNC('WEEK', claim_reported_at) AS claim_reported_at_week,
    LENGTH(TRIM(claim_type_slug)) AS claim_type_slug_len
FROM tagged
ORDER BY claim_claimed_amount
