WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        ROUND(approved_amount * 1.5, 2) AS approved_amount_adj,
        LENGTH(TRIM(claim_type)) AS claim_type_len,
        ABS(claimed_amount - 29985.9) AS claimed_amount_distance
    FROM claims
    WHERE policy_id IS NOT NULL
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN status IN ('IN_REVIEW', 'REJECTED') THEN 'group_a' WHEN status = 'APPROVED' THEN 'group_b' ELSE 'other' END AS status_group,
        FLOOR(approved_amount_adj / 10000) * 10000 AS approved_amount_adj_bucket,
        CONCAT(claim_type, '#', CAST(claim_type_len AS STRING)) AS claim_type_ref
    FROM filtered
)
-- Subtotals with CUBE
SELECT
    claim_type_len,
    COALESCE(status, 'ALL') AS status,
    policy_id,
    ROUND(SUM(approved_amount_adj_bucket) / NULLIF(SUM(approved_amount), 0), 3) AS approved_amount_adj_bucket_to_approved_amount_ra,
    MAX(approved_amount_adj_bucket) AS max_approved_amount_adj_bucket,
    ARRAY_SORT(COLLECT_SET(status)) AS status_list
FROM derived
GROUP BY CUBE(claim_type_len, status, policy_id)
