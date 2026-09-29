WITH filtered_claims AS (
    -- Keep valid rows and derive helper columns
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
        TRUNC(claim_date, 'MM') AS claim_date_month_start,
        approved_amount + claimed_amount AS approved_amount_plus_claimed_amount,
        GREATEST(claimed_amount, approved_amount) AS greatest_claimed_amount_approved_amount
    FROM claims
    WHERE claimed_amount < 43500.4
      AND TO_DATE(reported_at) < DATE '2025-03-08'
),
stats AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_ym,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list
    FROM filtered_claims
    WHERE claim_date_month_start >= DATE '2024-11-01'
    GROUP BY 1
),
per_label AS (
    -- One row per status_list_item
    SELECT s.claim_date_ym, t.status_list_item
    FROM stats s
    LATERAL VIEW EXPLODE(s.status_list) t AS status_list_item
)
SELECT
    claim_date_ym,
    status_list_item,
    CONCAT_WS(': ', claim_date_ym, status_list_item) AS claim_date_ym_status_list_item_label
FROM per_label
ORDER BY status_list_item DESC, claim_date_ym_status_list_item_label NULLS LAST, claim_date_ym
LIMIT 20
