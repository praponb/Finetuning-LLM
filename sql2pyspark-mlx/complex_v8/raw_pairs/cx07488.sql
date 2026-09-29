WITH recent_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        reported_at,
        UPPER(TRIM(claim_type)) AS claim_type_clean,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled,
        SUBSTRING(TRIM(status), 1, 2) AS status_prefix
    FROM claims
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        policy_id,
        claim_type,
        approved_amount,
        claimed_amount_filled,
        status_prefix,
        FLOOR(approved_amount / 10000) * 10000 AS approved_amount_bucket,
        SUBSTRING(TRIM(claim_type), 1, 2) AS claim_type_prefix
    FROM recent_claims
    WHERE claim_type_clean IS NOT NULL
)
-- Subtotals with ROLLUP
SELECT
    policy_id,
    COALESCE(claim_type, 'ALL') AS claim_type,
    MAX(approved_amount_bucket) AS max_approved_amount_bucket,
    COUNT(*) AS row_count,
    ROUND(SUM(claimed_amount_filled) / NULLIF(SUM(approved_amount), 0), 3) AS claimed_amount_filled_to_approved_amount_ratio
FROM scored
GROUP BY ROLLUP(policy_id, claim_type)
