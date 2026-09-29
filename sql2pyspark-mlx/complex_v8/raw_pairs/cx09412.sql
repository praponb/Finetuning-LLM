WITH joined_ranges AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.claim_type,
        c.status,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        policy_id,
        claimed_amount,
        status,
        product,
        CONCAT_WS('-', status, claim_type) AS status_claim_type_label,
        QUARTER(claim_date) AS claim_date_quarter,
        FLOOR(premium / 1000) * 1000 AS premium_bucket
    FROM joined_ranges
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(status_claim_type_label, '#', CAST(premium_bucket AS STRING)) AS status_claim_type_label_ref,
        CONCAT(status, '#', CAST(claim_date_quarter AS STRING)) AS status_ref,
        CASE WHEN status IN ('IN_REVIEW', 'OPEN') THEN 'group_a' WHEN status = 'PAID' THEN 'group_b' ELSE 'other' END AS status_group
    FROM enriched
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        product,
        claim_date_quarter,
        status_ref,
        status_group,
        UPPER(TRIM(status_ref)) AS status_ref_clean,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        SUBSTRING(TRIM(status_ref), 1, 4) AS status_ref_prefix
    FROM derived
)
-- Final projection
SELECT
    id,
    product,
    claim_date_quarter,
    status_ref,
    status_group,
    status_ref_clean,
    status_slug,
    status_ref_prefix,
    ABS(claim_date_quarter - 2) AS claim_date_quarter_distance,
    LENGTH(TRIM(product)) AS product_len
FROM labeled
ORDER BY product DESC NULLS FIRST
