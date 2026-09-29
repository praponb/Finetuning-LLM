WITH recent AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        reported_at,
        ROUND(claimed_amount * 1.5, 2) AS claimed_amount_adj,
        LOWER(TRIM(claim_type)) AS claim_type_clean
    FROM claims
    WHERE status NOT IN ('PAID', 'APPROVED')
      AND claimed_amount <= 31715.7
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        adjuster_id,
        claim_type,
        reported_at,
        claimed_amount_adj,
        claim_type_clean,
        SUBSTRING(TRIM(claim_type_clean), 1, 4) AS claim_type_clean_prefix,
        LOWER(REGEXP_REPLACE(TRIM(claim_type_clean), ' ', '_')) AS claim_type_clean_slug
    FROM recent
    WHERE claim_date < DATE '2024-12-20'
)
SELECT
    adjuster_id,
    claim_type,
    COUNT(DISTINCT adjuster_id) AS distinct_adjuster_ids,
    COUNT(*) AS cnt,
    MAX(reported_at) AS last_reported_at
FROM derived
GROUP BY ROLLUP(adjuster_id, claim_type)
ORDER BY adjuster_id NULLS LAST, claim_type NULLS LAST
