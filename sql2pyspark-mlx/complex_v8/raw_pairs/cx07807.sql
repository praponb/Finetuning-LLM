WITH recent_claims AS (
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
        UPPER(TRIM(status)) AS status_clean,
        COALESCE(approved_amount, 0.0) AS approved_amount_filled
    FROM claims
    WHERE approved_amount BETWEEN 2272.7 AND 21750.2
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY claim_type) AS rows_in_claim_type,
        MIN(claimed_amount) OVER (PARTITION BY status ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_claimed_amount_so_far,
        MIN(approved_amount_filled) OVER w_claim_type AS claim_type_min_approved_amount_filled,
        COUNT(*) OVER (PARTITION BY claim_type ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        NTILE(10) OVER (PARTITION BY claim_type ORDER BY approved_amount, id) AS approved_amount_decile,
        MIN(claimed_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date) ORDER BY approved_amount_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_claimed_amount_so_far_2,
        COUNT(*) OVER w_policy_id AS rows_in_policy_id,
        ROUND(PERCENT_RANK() OVER (ORDER BY reported_at ASC), 3) AS reported_at_percent_rank,
        ROUND(AVG(approved_amount_filled) OVER (PARTITION BY claim_type), 2) AS claim_type_avg_approved_amount_filled,
        LAG(claim_date) OVER (PARTITION BY status ORDER BY approved_amount DESC, id) AS prev_claim_date,
        approved_amount - LAG(approved_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date) ORDER BY approved_amount_filled DESC, id) AS approved_amount_change,
        MAX(claimed_amount) OVER (ORDER BY reported_at ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS claimed_amount_max_1_prior,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY claim_type)) AS distinct_status_in_claim_type,
        NTH_VALUE(approved_amount, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date) ORDER BY approved_amount_filled DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount
    FROM recent_claims
    WINDOW w_claim_type AS (PARTITION BY claim_type), w_policy_id AS (PARTITION BY policy_id)
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_FORMAT(claim_date, 'yyyy-MM') AS claim_date_label,
        ROUND(reported_at_percent_rank / NULLIF(rows_in_claim_type, 0), 3) AS reported_at_percent_rank_per_rows_in_claim_type,
        FLOOR(rows_in_claim_type / 5) * 5 AS rows_in_claim_type_bucket
    FROM with_windows
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER w_claim_date_month_by_distinct AS rn,
        LEAD(claim_date, 2) OVER (PARTITION BY distinct_status_in_claim_type, status ORDER BY reported_at_percent_rank_per_rows_in_claim_type, id) AS next2_claim_date,
        SUM(rows_in_claim_type_bucket) OVER (PARTITION BY MONTH(claim_date) ORDER BY distinct_status_in_claim_type DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rows_in_claim_type_bucket,
        LAST_VALUE(rows_in_claim_type) IGNORE NULLS OVER (PARTITION BY distinct_status_in_claim_type, status ORDER BY reported_at_percent_rank_per_rows_in_claim_type, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_rows_in_claim_type
    FROM tagged
    WINDOW w_claim_date_month_by_distinct AS (PARTITION BY MONTH(claim_date) ORDER BY distinct_status_in_claim_type DESC, id)
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    claim_type,
    claimed_amount,
    approved_amount,
    reported_at,
    status_clean,
    approved_amount_filled,
    rows_in_claim_type,
    min_claimed_amount_so_far,
    running_count,
    min_claimed_amount_so_far_2,
    rows_in_policy_id,
    reported_at_percent_rank,
    claim_type_avg_approved_amount_filled,
    prev_claim_date,
    claimed_amount_max_1_prior,
    distinct_status_in_claim_type,
    second_approved_amount,
    rows_in_claim_type_bucket,
    rn,
    running_rows_in_claim_type_bucket,
    last_known_rows_in_claim_type,
    ROUND(approved_amount_filled / NULLIF(second_approved_amount, 0), 3) AS approved_amount_filled_per_second_approved_amoun
FROM metrics
ORDER BY claim_type, claim_type_avg_approved_amount_filled
