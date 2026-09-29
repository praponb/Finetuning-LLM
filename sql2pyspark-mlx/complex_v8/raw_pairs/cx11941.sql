WITH valid_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown,
        INITCAP(TRIM(claim_type)) AS claim_type_clean
    FROM claims
    WHERE approved_amount < 23207.7
      AND status IN ('IN_REVIEW', 'OPEN', 'PAID')
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        claimed_amount,
        approved_amount,
        status,
        GREATEST(claimed_amount, approved_amount) AS greatest_claimed_amount_approved_amount,
        DATE_FORMAT(reported_at, 'yyyy-MM-dd') AS reported_at_label,
        LENGTH(TRIM(claim_type_slug)) AS claim_type_slug_len
    FROM valid_claims
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        claimed_amount,
        approved_amount,
        greatest_claimed_amount_approved_amount,
        claim_type_slug_len,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        ROUND(claimed_amount * 1.1, 2) AS claimed_amount_adj,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref
    FROM labeled
),
labeled_2 AS (
    -- Label and score rows
    SELECT
        id,
        claimed_amount,
        status_or_unknown,
        status_ref,
        CASE WHEN status_or_unknown IN ('REJECTED', 'APPROVED') THEN 'group_a' WHEN status_or_unknown = 'OPEN' THEN 'group_b' ELSE 'other' END AS status_or_unknown_group
    FROM annotated
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM labeled_2
    WHERE claimed_amount > (SELECT AVG(claimed_amount) FROM labeled_2)
),
derived AS (
    -- Label and score rows
    SELECT *, FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket
    FROM without_matches
    WHERE status_or_unknown_group IS NOT NULL
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        GREATEST(claimed_amount_bucket, claimed_amount) AS greatest_claimed_amount_bucket_claimed_amount,
        CASE WHEN claimed_amount >= 28900.9 THEN 'large' WHEN claimed_amount >= 18478.3 THEN 'medium' ELSE 'small' END AS claimed_amount_tier
    FROM derived
)
-- Final projection
SELECT
    id,
    status_or_unknown,
    status_ref,
    greatest_claimed_amount_bucket_claimed_amount,
    claimed_amount_tier,
    COALESCE(claimed_amount_tier, 'UNKNOWN') AS claimed_amount_tier_or_unknown
FROM tagged
WHERE claimed_amount >= 4868.7
ORDER BY claimed_amount_tier, greatest_claimed_amount_bucket_claimed_amount DESC
