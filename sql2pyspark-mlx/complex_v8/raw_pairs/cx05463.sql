WITH filtered_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM claims
    WHERE claim_type = 'theft'
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status_slug,
        SUBSTRING(TRIM(claim_type), 1, 4) AS claim_type_prefix,
        LENGTH(TRIM(status_slug)) AS status_slug_len
    FROM filtered_claims
    WHERE policy_id >= 6
),
with_details AS (
    -- Join policies
    SELECT
        s.*,
        p.holder_id AS policy_holder_id,
        p.start_date AS policy_start_date,
        p.end_date AS policy_end_date
    FROM derived s
    LEFT JOIN policies p ON p.id = s.policy_id
)
-- Subtotals with GROUPING SETS
SELECT
    claim_type,
    policy_holder_id,
    status_slug_len,
    GROUPING_ID() AS gid,
    MAX(policy_start_date) AS last_policy_start_date,
    SUM(approved_amount) AS total_approved_amount
FROM with_details
GROUP BY GROUPING SETS ((claim_type, policy_holder_id, status_slug_len), (claim_type), (policy_holder_id), ())
ORDER BY claim_type NULLS LAST, policy_holder_id NULLS LAST, status_slug_len NULLS LAST
