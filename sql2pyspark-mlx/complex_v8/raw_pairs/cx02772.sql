WITH cleaned AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled,
        DATE_TRUNC('MONTH', claim_date) AS claim_date_month
    FROM claims
    WHERE policy_id <= 17
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(claimed_amount_filled / NULLIF(SUM(claimed_amount_filled) OVER (PARTITION BY MONTH(claim_date)), 0), 4) AS claimed_amount_filled_share_of_claim_date_month,
        NTH_VALUE(claimed_amount, 2) OVER (PARTITION BY YEAR(claim_date_month), policy_id ORDER BY claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claimed_amount,
        LAG(claim_date_month, 2) OVER (PARTITION BY status, adjuster_id ORDER BY reported_at DESC, id) AS prev2_claim_date_month,
        RANK() OVER (PARTITION BY adjuster_id, DATE_TRUNC('MONTH', claim_date_month) ORDER BY claim_date DESC, id) AS claim_date_rank,
        ROW_NUMBER() OVER (PARTITION BY YEAR(claim_date_month), policy_id ORDER BY claimed_amount DESC, id) AS rn,
        LAST_VALUE(claim_date) OVER (PARTITION BY YEAR(claim_date_month), policy_id ORDER BY claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claim_date,
        RANK() OVER w_status_adjuster_id_by_report_2 AS reported_at_rank,
        COUNT(*) OVER w_claim_date_month_to_date_cla AS rows_in_claim_date_month_to_date_claim_type,
        NTH_VALUE(claimed_amount_filled, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', reported_at), claim_type ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claimed_amount_filled,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY MONTH(claim_date))) AS distinct_status_in_claim_date_month,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY policy_id, DATE_TRUNC('MONTH', reported_at))) AS distinct_claim_type_in_policy_id_reported_at_mon,
        claimed_amount_filled - LAG(claimed_amount_filled) OVER (PARTITION BY adjuster_id, DATE_TRUNC('MONTH', claim_date_month) ORDER BY claim_date DESC, id) AS claimed_amount_filled_change
    FROM cleaned
    WINDOW w_status_adjuster_id_by_report_2 AS (PARTITION BY status, adjuster_id ORDER BY reported_at DESC), w_claim_date_month_to_date_cla AS (PARTITION BY TO_DATE(claim_date_month), claim_type)
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claim_type,
    claimed_amount,
    status,
    reported_at,
    claimed_amount_filled,
    claimed_amount_filled_share_of_claim_date_month,
    second_claimed_amount,
    prev2_claim_date_month,
    rn,
    last_claim_date,
    reported_at_rank,
    rows_in_claim_date_month_to_date_claim_type,
    second_claimed_amount_filled,
    distinct_status_in_claim_date_month
FROM metrics
WHERE distinct_claim_type_in_policy_id_reported_at_mon < 9
ORDER BY adjuster_id NULLS LAST
