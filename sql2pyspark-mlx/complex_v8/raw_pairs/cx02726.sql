WITH filtered AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        CASE WHEN approved_amount >= 19337.6 THEN 'A' WHEN approved_amount >= 0.0 THEN 'B' ELSE 'C' END AS approved_amount_tier,
        CASE WHEN approved_amount >= 20880.4 THEN 'HIGH' WHEN approved_amount >= 2569.2 THEN 'MEDIUM' ELSE 'LOW' END AS approved_amount_tier_2
    FROM claims
    WHERE reported_at < TIMESTAMP '2025-02-24 07:00:00'
      AND status = 'APPROVED'
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
        )
),
per_group AS (
    -- Summarise per group
    SELECT
        approved_amount_tier_2,
        YEAR(claim_date) AS claim_date_year,
        status,
        COUNT(*) FILTER (WHERE status <> 'REJECTED') AS n_status_rejected,
        MAX(approved_amount) AS max_approved_amount,
        MIN(claim_date) AS first_claim_date,
        SUM(CASE WHEN status IN ('IN_REVIEW', 'OPEN', 'PAID') THEN 1 ELSE 0 END) AS n_status_in_in_review_open_pai,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list
    FROM qualified
    GROUP BY 1, 2, 3
),
tagged AS (
    -- Add calculated columns
    SELECT
        approved_amount_tier_2,
        claim_date_year,
        status,
        n_status_rejected,
        max_approved_amount,
        n_status_in_in_review_open_pai,
        claim_type_list,
        SIZE(claim_type_list) AS claim_type_list_count
    FROM per_group
    WHERE SIZE(claim_type_list) >= 1
)
SELECT
    approved_amount_tier_2,
    status,
    n_status_rejected,
    max_approved_amount,
    claim_type_list,
    claim_type_list_count,
    n_status_rejected - claim_type_list_count AS n_status_rejected_minus_claim_type_list_count
FROM tagged
ORDER BY approved_amount_tier_2, max_approved_amount
