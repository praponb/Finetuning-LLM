WITH filtered AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        reported_at,
        TRUNC(claim_date, 'MM') AS claim_date_month_start,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown,
        approved_amount - claimed_amount AS approved_amount_minus_claimed_amount
    FROM claims
    WHERE approved_amount <= 34401.0
      AND reported_at < TIMESTAMP '2025-02-24 07:00:00'
),
with_details AS (
    -- Attach policy attributes
    SELECT s.*, p.start_date AS policy_start_date
    FROM filtered s
    JOIN policies p ON p.id = s.policy_id
    WHERE p.start_date < DATE '2025-05-02'
),
enriched_policy AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        s.reported_at,
        s.claim_type_or_unknown,
        s.policy_start_date,
        p.product AS policy_product,
        p.start_date AS policy_start_date_2,
        po.segment AS holder_segment,
        po.region AS holder_region
    FROM with_details s
    JOIN policies p ON p.id = s.policy_id
    JOIN policyholders po ON po.id = p.holder_id
),
with_activity AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM enriched_policy s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.claim_type <> 'medical'
        )
      AND s.claim_type_or_unknown NOT IN ('theft', 'flood')
)
-- Final projection
SELECT
    id,
    adjuster_id,
    claim_type,
    approved_amount,
    reported_at,
    claim_type_or_unknown,
    policy_start_date,
    policy_product,
    policy_start_date_2,
    holder_region,
    DATE_ADD(claim_date, 365) AS claim_date_plus_365d,
    LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug
FROM with_activity
ORDER BY claim_type_slug NULLS LAST, adjuster_id DESC, id
LIMIT 10
