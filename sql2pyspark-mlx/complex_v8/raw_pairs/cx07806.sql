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
        CONCAT(status, '#', CAST(policy_id AS STRING)) AS status_ref,
        ABS(claimed_amount - 18364.1) AS claimed_amount_distance
    FROM claims
    WHERE status = 'OPEN'
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        claimed_amount_distance,
        CASE WHEN claim_type IN ('theft', 'fire') THEN 'group_a' WHEN claim_type = 'collision' THEN 'group_b' ELSE 'other' END AS claim_type_group
    FROM cleaned
),
totals AS (
    -- Summarise per group
    SELECT
        status,
        adjuster_id,
        claim_type,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list,
        SUM(claimed_amount) AS total_claimed_amount,
        COUNT(DISTINCT claim_type) AS distinct_claim_types,
        COUNT(*) FILTER (WHERE status IN ('REJECTED', 'IN_REVIEW')) AS n_status_in_rejected_in_review
    FROM enriched
    WHERE status = 'PAID'
    GROUP BY status, adjuster_id, claim_type
),
enriched_2 AS (
    -- Add calculated columns
    SELECT
        status,
        adjuster_id,
        claim_type,
        status_list,
        distinct_claim_types,
        n_status_in_rejected_in_review,
        FLOOR(distinct_claim_types / 2) * 2 AS distinct_claim_types_bucket,
        INITCAP(TRIM(status)) AS status_clean,
        FLOOR(distinct_claim_types / 2) * 2 AS distinct_claim_types_bucket_2
    FROM totals
    WHERE claim_type IN ('medical', 'delay', 'flood')
)
-- Final projection
SELECT
    status,
    adjuster_id,
    claim_type,
    status_list,
    distinct_claim_types,
    distinct_claim_types_bucket,
    distinct_claim_types_bucket_2,
    UPPER(TRIM(status_clean)) AS status_clean_clean,
    SUBSTRING(TRIM(status_clean), 1, 4) AS status_clean_prefix
FROM enriched_2
ORDER BY claim_type, status
