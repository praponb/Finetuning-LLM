WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM claims
    WHERE (status = 'PAID' OR claimed_amount <= 29191.2)
),
combined_details AS (
    -- Enrich with dimension data
    SELECT s.*, p.start_date AS policy_start_date, p.premium AS policy_premium
    FROM scoped s
    LEFT JOIN policies p ON p.id = s.policy_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), claim_type), 0), 4) AS claimed_amount_share_of_claim_date_month_claim_t,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), claim_type), 2) AS claimed_amount_vs_claim_date_month_claim_type_av,
        ROUND(PERCENT_RANK() OVER (PARTITION BY claim_type ORDER BY approved_amount DESC), 3) AS approved_amount_percent_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(reported_at) ORDER BY claimed_amount, id), 3) AS claimed_amount_percent_rank,
        DENSE_RANK() OVER w_adjuster_id_policy_start_dat AS reported_at_dense_rank,
        LAST_VALUE(claim_date) OVER (PARTITION BY claim_type ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claim_date,
        ROUND(PERCENT_RANK() OVER (PARTITION BY adjuster_id, MONTH(claim_date) ORDER BY claimed_amount DESC NULLS FIRST), 3) AS claimed_amount_percent_rank_2,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), claim_type) AS rows_in_claim_date_month_claim_type,
        LEAD(claim_date) OVER (PARTITION BY adjuster_id, YEAR(policy_start_date) ORDER BY reported_at DESC, id) AS next_claim_date,
        MIN(approved_amount) OVER w_claim_date_month_claim_type AS claim_date_month_claim_type_min_approved_amount,
        ROW_NUMBER() OVER (PARTITION BY adjuster_id, MONTH(claim_date) ORDER BY claimed_amount DESC NULLS FIRST, id) AS seq_no,
        ROUND(policy_premium - AVG(policy_premium) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), claim_type), 2) AS policy_premium_vs_claim_date_month_claim_type_av,
        LAST_VALUE(approved_amount) OVER (PARTITION BY adjuster_id, YEAR(policy_start_date) ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_approved_amount,
        FIRST_VALUE(claimed_amount) OVER (PARTITION BY claim_type ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claimed_amount
    FROM combined_details
    WINDOW w_adjuster_id_policy_start_dat AS (PARTITION BY adjuster_id, YEAR(policy_start_date) ORDER BY reported_at DESC), w_claim_date_month_claim_type AS (PARTITION BY DATE_TRUNC('MONTH', claim_date), claim_type)
),
first_rows AS (
    -- Top 5 per group
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
        policy_start_date,
        policy_premium,
        claimed_amount_share_of_claim_date_month_claim_t,
        claimed_amount_vs_claim_date_month_claim_type_av,
        approved_amount_percent_rank,
        claimed_amount_percent_rank,
        last_claim_date,
        claimed_amount_percent_rank_2,
        rows_in_claim_date_month_claim_type,
        next_claim_date,
        claim_date_month_claim_type_min_approved_amount,
        seq_no,
        policy_premium_vs_claim_date_month_claim_type_av,
        last_approved_amount,
        first_claimed_amount
    FROM metrics
    WHERE reported_at_dense_rank <= 5
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claim_type,
    claimed_amount,
    approved_amount,
    reported_at,
    policy_start_date,
    policy_premium,
    claimed_amount_share_of_claim_date_month_claim_t,
    approved_amount_percent_rank,
    claimed_amount_percent_rank,
    last_claim_date,
    claimed_amount_percent_rank_2,
    rows_in_claim_date_month_claim_type,
    seq_no,
    policy_premium_vs_claim_date_month_claim_type_av,
    first_claimed_amount,
    DATEDIFF(DATE '2025-06-30', next_claim_date) AS days_since_next_claim_date
FROM first_rows
ORDER BY days_since_next_claim_date DESC, seq_no
