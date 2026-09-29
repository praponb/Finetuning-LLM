WITH recent_claims AS (
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
        DATEDIFF(DATE '2025-12-31', claim_date) AS days_since_claim_date,
        DATE_TRUNC('YEAR', claim_date) AS claim_date_year
    FROM claims
    WHERE approved_amount < 15477.7
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(days_since_claim_date / NULLIF(SUM(days_since_claim_date) OVER (PARTITION BY status, claim_type), 0), 4) AS days_since_claim_date_share_of_status_claim_type,
        NTH_VALUE(claimed_amount, 2) OVER (ORDER BY days_since_claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claimed_amount,
        SUM(days_since_claim_date) OVER (PARTITION BY status, claim_type) AS status_claim_type_sum_days_since_claim_date,
        ROW_NUMBER() OVER (ORDER BY days_since_claim_date DESC, id) AS seq_no,
        LEAD(reported_at) OVER (ORDER BY days_since_claim_date DESC, id) AS next_reported_at,
        FIRST_VALUE(claim_date) OVER (PARTITION BY status, adjuster_id ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claim_date,
        DENSE_RANK() OVER w_all_by_days_since_claim_date_3 AS days_since_claim_date_dense_rank,
        MAX(days_since_claim_date) OVER (PARTITION BY status, claim_type) AS status_claim_type_max_days_since_claim_date,
        MAX(claimed_amount) OVER (PARTITION BY adjuster_id ORDER BY claim_date_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_claimed_amount_so_far,
        DENSE_RANK() OVER w_adjuster_id_by_claim_date_ye_2 AS claim_date_year_dense_rank,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY status, claim_type), 2) AS approved_amount_vs_status_claim_type_avg,
        ROW_NUMBER() OVER (PARTITION BY status, adjuster_id ORDER BY claim_date DESC, id) AS seq_no_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY adjuster_id ORDER BY claim_date_year DESC), 3) AS claim_date_year_percent_rank,
        COUNT(*) OVER (PARTITION BY status, claim_type) AS rows_in_status_claim_type
    FROM recent_claims
    WINDOW w_all_by_days_since_claim_date_3 AS (ORDER BY days_since_claim_date DESC), w_adjuster_id_by_claim_date_ye_2 AS (PARTITION BY adjuster_id ORDER BY claim_date_year DESC)
),
leaders AS (
    -- Top 5 per group
    SELECT *
    FROM ranked
    WHERE seq_no <= 5
      AND status_claim_type_sum_days_since_claim_date >= 1050
)
SELECT
    id,
    adjuster_id,
    claim_date,
    claim_type,
    claimed_amount,
    approved_amount,
    reported_at,
    days_since_claim_date,
    claim_date_year,
    days_since_claim_date_share_of_status_claim_type,
    second_claimed_amount,
    status_claim_type_sum_days_since_claim_date,
    seq_no,
    next_reported_at,
    first_claim_date,
    days_since_claim_date_dense_rank,
    status_claim_type_max_days_since_claim_date,
    max_claimed_amount_so_far,
    claim_date_year_dense_rank,
    approved_amount_vs_status_claim_type_avg,
    seq_no_2,
    claim_date_year_percent_rank,
    rows_in_status_claim_type,
    LEAST(second_claimed_amount, approved_amount) AS least_second_claimed_amount_approved_amount
FROM leaders
