WITH cleaned_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        LENGTH(TRIM(status)) AS status_len,
        SUBSTRING(TRIM(claim_type), 1, 3) AS claim_type_prefix
    FROM claims
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        claim_type_prefix,
        LOWER(REGEXP_REPLACE(TRIM(claim_type_prefix), ' ', '_')) AS claim_type_prefix_slug,
        CONCAT(claim_type_prefix, '#', CAST(adjuster_id AS STRING)) AS claim_type_prefix_ref
    FROM cleaned_claims
    WHERE adjuster_id <= 2
)
SELECT
    policy_id,
    adjuster_id,
    ROUND(STDDEV(claimed_amount), 2) AS stddev_claimed_amount,
    COUNT(*) AS n_rows,
    PERCENTILE_APPROX(claimed_amount, 0.5) AS median_claimed_amount
FROM enriched
GROUP BY ROLLUP(policy_id, adjuster_id)
ORDER BY policy_id NULLS LAST, adjuster_id NULLS LAST
