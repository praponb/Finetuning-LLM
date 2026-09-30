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
        DATE_FORMAT(reported_at, 'yyyy-MM') AS reported_at_label,
        DATE_FORMAT(reported_at, 'yyyy-MM-dd HH:00') AS reported_at_label_2
    FROM claims
    WHERE policy_id IS NOT NULL
      AND claim_date >= DATE '2024-06-20'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        reported_at_label,
        FIRST_VALUE(claim_date) OVER (PARTITION BY policy_id ORDER BY approved_amount DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claim_date,
        COUNT(*) OVER (PARTITION BY YEAR(claim_date), claim_type) AS rows_in_claim_date_year_claim_type,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY status), 2) AS approved_amount_vs_status_avg,
        COUNT(*) OVER (PARTITION BY claim_type, policy_id ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LAST_VALUE(claimed_amount) IGNORE NULLS OVER (PARTITION BY claim_type ORDER BY claim_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claimed_amount,
        LAST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY claim_type, policy_id ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claim_date,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY status, claim_type)) AS distinct_policy_id_in_status_claim_type,
        ROUND(AVG(approved_amount) OVER (PARTITION BY adjuster_id, claim_type ORDER BY approved_amount ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS approved_amount_avg_2_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY claim_type, policy_id ORDER BY claim_date), 3) AS claim_date_percent_rank,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY status), 0), 4) AS approved_amount_share_of_status,
        ROUND(AVG(approved_amount) OVER (PARTITION BY policy_id ORDER BY approved_amount DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS approved_amount_avg_6_prior,
        LAST_VALUE(claimed_amount) OVER (PARTITION BY claim_type ORDER BY claim_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claimed_amount,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY claim_type, policy_id ORDER BY claim_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS claimed_amount_avg_2_prior,
        ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY approved_amount DESC NULLS FIRST, id) AS seq_no,
        ROW_NUMBER() OVER (ORDER BY claim_date DESC, id) AS claim_date_seq
    FROM filtered
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        adjuster_id,
        approved_amount,
        status,
        first_claim_date,
        rows_in_claim_date_year_claim_type,
        last_known_claimed_amount,
        last_known_claim_date,
        distinct_policy_id_in_status_claim_type,
        approved_amount_avg_2_prior,
        approved_amount_share_of_status,
        approved_amount_avg_6_prior,
        last_claimed_amount,
        claimed_amount_avg_2_prior,
        ROUND(AVG(approved_amount_avg_6_prior) OVER (ORDER BY claim_date_seq DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS approved_amount_avg_6_prior_avg_1_prior,
        SUM(running_count) OVER (PARTITION BY seq_no) AS seq_no_sum_running_count,
        LAST_VALUE(claim_date_seq) IGNORE NULLS OVER (ORDER BY claim_date_seq DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claim_date_seq,
        DENSE_RANK() OVER (ORDER BY claim_date_seq DESC NULLS FIRST) AS claim_date_seq_dense_rank,
        NTH_VALUE(claimed_amount_avg_2_prior, 2) OVER (ORDER BY claim_date_seq DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claimed_amount_avg_2_prior
    FROM with_windows
)
SELECT
    id,
    adjuster_id,
    status,
    first_claim_date,
    rows_in_claim_date_year_claim_type,
    last_known_claimed_amount,
    last_known_claim_date,
    distinct_policy_id_in_status_claim_type,
    approved_amount_avg_2_prior,
    approved_amount_share_of_status,
    approved_amount_avg_6_prior,
    claimed_amount_avg_2_prior,
    approved_amount_avg_6_prior_avg_1_prior,
    last_known_claim_date_seq,
    claim_date_seq_dense_rank,
    second_claimed_amount_avg_2_prior,
    COALESCE(status, 'UNKNOWN') AS status_or_unknown
FROM windowed
ORDER BY adjuster_id, id
LIMIT 20
