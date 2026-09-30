WITH base_claims AS (
    -- Filter claims
    SELECT *
    FROM claims
    WHERE claim_date IS NOT NULL
      AND HOUR(reported_at) BETWEEN 10 AND 16
),
with_details_policy AS (
    -- Attach policy attributes
    SELECT s.*, p.start_date AS policy_start_date
    FROM base_claims s
    JOIN policies p ON p.id = s.policy_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        policy_start_date,
        SUM(claimed_amount) OVER (PARTITION BY MONTH(reported_at), policy_id ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_claimed_amount,
        COUNT(*) OVER (PARTITION BY status, claim_type) AS rows_in_status_claim_type,
        LAG(approved_amount) OVER (PARTITION BY YEAR(policy_start_date) ORDER BY claim_date, id) AS prev_approved_amount,
        NTH_VALUE(approved_amount, 2) OVER (PARTITION BY MONTH(reported_at), policy_id ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY status, claim_type), 0), 4) AS approved_amount_share_of_status_claim_type,
        ROUND(claimed_amount - AVG(claimed_amount) OVER w_status_claim_type, 2) AS claimed_amount_vs_status_claim_type_avg,
        SIZE(COLLECT_SET(policy_id) OVER w_status_claim_type) AS distinct_policy_id_in_status_claim_type,
        DATEDIFF(policy_start_date, LAG(policy_start_date) OVER (PARTITION BY YEAR(policy_start_date) ORDER BY claim_date, id)) AS days_since_prev_policy_start_date,
        MAX(claimed_amount) OVER (PARTITION BY status, claim_type) AS status_claim_type_max_claimed_amount,
        NTH_VALUE(policy_start_date, 2) OVER (PARTITION BY MONTH(reported_at), policy_id ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_policy_start_date,
        MIN(claimed_amount) OVER w_status_claim_type AS status_claim_type_min_claimed_amount,
        ROUND(PERCENT_RANK() OVER w_policy_start_date_year_by_cl, 3) AS claim_date_percent_rank,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY MONTH(reported_at), policy_id ORDER BY reported_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS claimed_amount_avg_6_prior
    FROM with_details_policy
    WINDOW w_status_claim_type AS (PARTITION BY status, claim_type), w_policy_start_date_year_by_cl AS (PARTITION BY YEAR(policy_start_date) ORDER BY claim_date, id)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY MONTH(claim_date), MONTH(policy_start_date) ORDER BY claim_date_percent_rank ASC, id) AS rn,
        DATEDIFF(policy_start_date, LAG(policy_start_date) OVER (PARTITION BY MONTH(claim_date), MONTH(policy_start_date) ORDER BY claim_date_percent_rank ASC, id)) AS days_since_prev_policy_start_date_2,
        NTH_VALUE(second_policy_start_date, 2) OVER (PARTITION BY claim_type ORDER BY rows_in_status_claim_type ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_second_policy_start_date,
        NTILE(10) OVER (PARTITION BY distinct_policy_id_in_status_claim_type, DATE_TRUNC('MONTH', reported_at) ORDER BY days_since_prev_policy_start_date NULLS LAST, id) AS days_since_prev_policy_start_date_decile,
        LEAD(claim_date_percent_rank, 1, 0) OVER (PARTITION BY distinct_policy_id_in_status_claim_type, DATE_TRUNC('MONTH', reported_at) ORDER BY days_since_prev_policy_start_date NULLS LAST, id) AS next_claim_date_percent_rank,
        ROUND(claim_date_percent_rank / NULLIF(SUM(claim_date_percent_rank) OVER (PARTITION BY distinct_policy_id_in_status_claim_type, status), 0), 4) AS claim_date_percent_rank_share_of_distinct_policy,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(claim_date), MONTH(policy_start_date) ORDER BY claim_date_percent_rank ASC), 3) AS claim_date_percent_rank_cume_dist
    FROM windowed
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_type,
    approved_amount,
    reported_at,
    running_claimed_amount,
    rows_in_status_claim_type,
    status_claim_type_max_claimed_amount,
    status_claim_type_min_claimed_amount,
    claimed_amount_avg_6_prior,
    rn,
    days_since_prev_policy_start_date_2,
    second_second_policy_start_date,
    next_claim_date_percent_rank,
    claim_date_percent_rank_cume_dist,
    SUBSTRING(TRIM(status), 1, 4) AS status_prefix,
    COALESCE(status, 'UNKNOWN') AS status_or_unknown
FROM with_windows
ORDER BY claim_type DESC, status_prefix DESC
