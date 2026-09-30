WITH scoped AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        reported_at,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM claims
    WHERE claim_date IS NOT NULL
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY claim_type), 2) AS approved_amount_vs_claim_type_avg,
        MAX(approved_amount) OVER (PARTITION BY claim_type ORDER BY approved_amount DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS approved_amount_max_3,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status_or_unknown, claim_type ORDER BY claim_date DESC), 3) AS claim_date_percent_rank,
        SUM(approved_amount) OVER (PARTITION BY adjuster_id) AS adjuster_id_sum_approved_amount,
        COUNT(*) OVER (PARTITION BY claim_type) AS rows_in_claim_type,
        DENSE_RANK() OVER (PARTITION BY TO_DATE(reported_at), policy_id ORDER BY reported_at, id) AS reported_at_dense_rank,
        COUNT(*) OVER (PARTITION BY adjuster_id, DATE_TRUNC('MONTH', reported_at)) AS rows_in_adjuster_id_reported_at_month,
        NTH_VALUE(approved_amount, 2) OVER (ORDER BY reported_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount,
        SUM(approved_amount) OVER (PARTITION BY adjuster_id, DATE_TRUNC('MONTH', reported_at)) AS adjuster_id_reported_at_month_sum_approved_amoun,
        DENSE_RANK() OVER (PARTITION BY claim_type ORDER BY approved_amount DESC) AS approved_amount_dense_rank,
        MAX(approved_amount) OVER (PARTITION BY adjuster_id, DATE_TRUNC('MONTH', reported_at)) AS adjuster_id_reported_at_month_max_approved_amoun,
        NTH_VALUE(claim_date, 2) OVER (PARTITION BY claim_type ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date,
        ROUND(PERCENT_RANK() OVER (ORDER BY reported_at NULLS LAST, id), 3) AS reported_at_percent_rank,
        FIRST_VALUE(approved_amount) OVER (PARTITION BY claim_type ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_approved_amount,
        ROUND(AVG(approved_amount) OVER (PARTITION BY TO_DATE(reported_at), policy_id ORDER BY reported_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS approved_amount_avg_4
    FROM scoped
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    claim_type,
    approved_amount,
    reported_at,
    status_or_unknown,
    approved_amount_vs_claim_type_avg,
    claim_date_percent_rank,
    rows_in_claim_type,
    reported_at_dense_rank,
    rows_in_adjuster_id_reported_at_month,
    second_approved_amount,
    adjuster_id_reported_at_month_sum_approved_amoun,
    approved_amount_dense_rank,
    adjuster_id_reported_at_month_max_approved_amoun,
    second_claim_date,
    reported_at_percent_rank,
    first_approved_amount,
    approved_amount_avg_4,
    DATE_TRUNC('QUARTER', second_claim_date) AS second_claim_date_quarter
FROM ranked
ORDER BY second_approved_amount NULLS LAST, rows_in_adjuster_id_reported_at_month DESC, id
LIMIT 20
