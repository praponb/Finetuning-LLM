WITH filtered_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled
    FROM claims
    WHERE status IN ('OPEN', 'PAID')
      AND adjuster_id >= 6
),
stats AS (
    -- Group-level totals
    SELECT
        claim_type,
        ROUND(100 * COUNT(CASE WHEN claim_type = 'medical' THEN 1 END) / COUNT(*), 1) AS pct_claim_type_medical,
        COUNT(*) AS n_rows,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
        MIN(approved_amount) AS min_approved_amount
    FROM filtered_claims
    GROUP BY claim_type
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(pct_claim_type_medical * 1.1, 2) AS pct_claim_type_medical_adj,
        ROUND(pct_claim_type_medical * 0.9, 2) AS pct_claim_type_medical_adj_2
    FROM stats
    WHERE n_rows BETWEEN 3 AND 10
)
SELECT claim_type, measure, reading
FROM (
    SELECT claim_type, pct_claim_type_medical, min_approved_amount, pct_claim_type_medical_adj
    FROM annotated
) src
UNPIVOT INCLUDE NULLS (
    reading FOR measure IN (pct_claim_type_medical, min_approved_amount, pct_claim_type_medical_adj)
) unp
