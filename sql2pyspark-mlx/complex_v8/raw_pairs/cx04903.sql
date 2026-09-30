WITH cleaned AS (
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
        ABS(approved_amount - 20843.1) AS approved_amount_distance,
        CONCAT_WS('|', status, claim_type) AS status_claim_type_label,
        GREATEST(approved_amount, claimed_amount) AS greatest_approved_amount_claimed_amount
    FROM claims
    WHERE adjuster_id IS NOT NULL
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', reported_at) AS reported_at_month,
        DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_ym,
        ROUND(100 * COUNT(CASE WHEN status = 'PAID' THEN 1 END) / COUNT(*), 1) AS pct_status_paid,
        COUNT(*) AS row_count,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list
    FROM cleaned
    GROUP BY DATE_TRUNC('MONTH', reported_at), DATE_FORMAT(claim_date, 'yyyy-MM')
    HAVING COUNT(*) >= 3
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        DATEDIFF(DATE '2025-06-30', reported_at_month) AS days_since_reported_at_month,
        ROUND(pct_status_paid * 0.9, 2) AS pct_status_paid_adj,
        FLOOR(row_count / 2) * 2 AS row_count_bucket
    FROM rollup_base
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM labeled
    WHERE pct_status_paid_adj > (SELECT AVG(pct_status_paid_adj) FROM labeled)
),
labeled_2 AS (
    -- Derive helper columns
    SELECT *, ARRAY_JOIN(ARRAY_SORT(claim_type_list), ',') AS claim_type_list_text
    FROM without_matches
)
SELECT reported_at_month, claim_date_ym, metric, amount
FROM (
    SELECT reported_at_month, claim_date_ym, days_since_reported_at_month, row_count
    FROM labeled_2
) src
UNPIVOT (
    amount FOR metric IN (days_since_reported_at_month, row_count)
) unp
