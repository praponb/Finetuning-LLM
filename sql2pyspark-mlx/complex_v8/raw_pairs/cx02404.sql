WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        MONTH(claim_date) AS claim_date_month
    FROM claims
    WHERE claimed_amount < 44921.3
      AND claim_type <> 'theft'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(claim_date_month) OVER (PARTITION BY status, claim_type ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_claim_date_month_so_far,
        ROUND(claim_date_month - AVG(claim_date_month) OVER (PARTITION BY claim_type), 2) AS claim_date_month_vs_claim_type_avg,
        ROUND(claimed_amount - AVG(claimed_amount) OVER w_claim_type, 2) AS claimed_amount_vs_claim_type_avg,
        ROUND(AVG(claimed_amount) OVER (ORDER BY claim_date_month NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS claimed_amount_avg_6_prior,
        COUNT(*) OVER (PARTITION BY claim_type) AS rows_in_claim_type,
        LAST_VALUE(claim_date) IGNORE NULLS OVER (ORDER BY claim_date_month NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claim_date,
        ROUND(AVG(claimed_amount) OVER w_claim_type, 2) AS claim_type_avg_claimed_amount,
        FIRST_VALUE(reported_at) OVER (ORDER BY claim_date_month NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_reported_at,
        MAX(claim_date_month) OVER (PARTITION BY claim_type) AS claim_type_max_claim_date_month,
        claimed_amount - LAG(claimed_amount) OVER (PARTITION BY status, claim_type ORDER BY claim_date, id) AS claimed_amount_change,
        ROUND(CUME_DIST() OVER (PARTITION BY status, claim_type ORDER BY claim_date, id), 3) AS claim_date_cume_dist,
        ROW_NUMBER() OVER (ORDER BY claim_date_month NULLS LAST, id) AS claim_date_month_seq
    FROM cleaned
    WINDOW w_claim_type AS (PARTITION BY claim_type)
),
leaders AS (
    -- First row per group
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        claim_date_month,
        min_claim_date_month_so_far,
        claim_date_month_vs_claim_type_avg,
        claimed_amount_vs_claim_type_avg,
        claimed_amount_avg_6_prior,
        rows_in_claim_type,
        last_known_claim_date,
        claim_type_avg_claimed_amount,
        first_reported_at,
        claim_type_max_claim_date_month,
        claimed_amount_change,
        claim_date_cume_dist
    FROM metrics
    WHERE claim_date_month_seq = 1
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (PARTITION BY policy_id, status ORDER BY claim_date_cume_dist, id) AS claim_date_cume_dist_rank,
        LAG(rows_in_claim_type, 2, 0) OVER (PARTITION BY claim_date_month ORDER BY claim_type_avg_claimed_amount DESC, id) AS prev2_rows_in_claim_type,
        ROUND(PERCENT_RANK() OVER (PARTITION BY claim_date_month ORDER BY claim_type_avg_claimed_amount DESC, id), 3) AS claim_type_avg_claimed_amount_percent_rank,
        ROUND(claimed_amount_avg_6_prior - AVG(claimed_amount_avg_6_prior) OVER w_reported_at_to_date_claim_da, 2) AS claimed_amount_avg_6_prior_vs_reported_at_to_dat
    FROM leaders
    WINDOW w_reported_at_to_date_claim_da AS (PARTITION BY TO_DATE(reported_at), claim_date_month)
)
-- Final projection
SELECT DISTINCT min_claim_date_month_so_far, adjuster_id, id
FROM sequenced
WHERE claimed_amount_change >= -25000.0
