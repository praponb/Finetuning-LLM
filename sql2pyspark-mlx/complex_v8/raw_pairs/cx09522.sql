WITH valid AS (
    -- Filter claims
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
        LOWER(TRIM(status)) AS status_clean
    FROM claims
    WHERE (adjuster_id <= 4 OR policy_id IS NOT NULL)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY MONTH(reported_at) ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        COUNT(*) OVER (PARTITION BY adjuster_id, claim_type ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_2,
        DENSE_RANK() OVER w_policy_id_adjuster_id_by_cla AS claimed_amount_dense_rank,
        NTILE(5) OVER (PARTITION BY policy_id, adjuster_id ORDER BY claimed_amount ASC, id) AS claimed_amount_quintile,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER w_claim_type_policy_id, 0), 4) AS approved_amount_share_of_claim_type_policy_id,
        RANK() OVER (ORDER BY claim_date, id) AS claim_date_rank,
        ROUND(PERCENT_RANK() OVER w_all_by_claim_date_ties, 3) AS claim_date_percent_rank,
        MIN(approved_amount) OVER (PARTITION BY claim_type, policy_id) AS claim_type_policy_id_min_approved_amount,
        ROW_NUMBER() OVER w_reported_at_month_by_claim_d_2 AS rn,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY claim_type, policy_id), 0), 4) AS claimed_amount_share_of_claim_type_policy_id,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY policy_id, status)) AS distinct_claim_type_in_policy_id_status,
        ROUND((UNIX_TIMESTAMP(reported_at) - UNIX_TIMESTAMP(LAG(reported_at) OVER w_adjuster_id_status_by_report)) / 60, 1) AS minutes_since_prev_reported_at,
        claimed_amount - LAG(claimed_amount) OVER (PARTITION BY policy_id, adjuster_id ORDER BY claimed_amount ASC, id) AS claimed_amount_change,
        MAX(approved_amount) OVER (PARTITION BY policy_id, adjuster_id ORDER BY claimed_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_approved_amount_so_far
    FROM valid
    WINDOW w_policy_id_adjuster_id_by_cla AS (PARTITION BY policy_id, adjuster_id ORDER BY claimed_amount ASC), w_claim_type_policy_id AS (PARTITION BY claim_type, policy_id), w_all_by_claim_date_ties AS (ORDER BY claim_date), w_reported_at_month_by_claim_d_2 AS (PARTITION BY MONTH(reported_at) ORDER BY claim_date DESC, id), w_adjuster_id_status_by_report AS (PARTITION BY adjuster_id, status ORDER BY reported_at ASC, id)
),
derived AS (
    -- Add calculated columns
    SELECT *, LENGTH(TRIM(claim_type)) AS claim_type_len
    FROM with_windows
    WHERE status_clean IS NOT NULL
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY claimed_amount_dense_rank), 0), 4) AS claimed_amount_share_of_claimed_amount_dense_ran,
        FIRST_VALUE(claimed_amount_quintile) OVER (PARTITION BY claimed_amount_quintile, YEAR(reported_at) ORDER BY claimed_amount_share_of_claim_type_policy_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claimed_amount_quintile,
        ROW_NUMBER() OVER w_claimed_amount_quintile_repo_2 AS seq_no,
        ROUND(claim_date_percent_rank - AVG(claim_date_percent_rank) OVER (PARTITION BY claimed_amount_dense_rank), 2) AS claim_date_percent_rank_vs_claimed_amount_dense_
    FROM derived
    WINDOW w_claimed_amount_quintile_repo_2 AS (PARTITION BY claimed_amount_quintile, YEAR(reported_at) ORDER BY claimed_amount_share_of_claim_type_policy_id DESC, id)
)
-- Final projection
SELECT
    id,
    adjuster_id,
    claim_date,
    claim_type,
    claimed_amount,
    status,
    reported_at,
    status_clean,
    running_count,
    running_count_2,
    claimed_amount_dense_rank,
    claimed_amount_quintile,
    approved_amount_share_of_claim_type_policy_id,
    claim_date_percent_rank,
    claim_type_policy_id_min_approved_amount,
    rn,
    claimed_amount_share_of_claim_type_policy_id,
    minutes_since_prev_reported_at,
    claimed_amount_change,
    max_approved_amount_so_far,
    claim_type_len,
    claimed_amount_share_of_claimed_amount_dense_ran,
    first_claimed_amount_quintile,
    claim_date_percent_rank_vs_claimed_amount_dense_,
    ROUND(running_count / NULLIF(approved_amount, 0), 3) AS running_count_per_approved_amount,
    LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
FROM with_trends
