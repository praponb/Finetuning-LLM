WITH recent_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        reported_at,
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket
    FROM claims
    WHERE claimed_amount <= 31715.7
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.approved_amount,
        s.reported_at,
        s.claimed_amount_bucket,
        p.premium AS policy_premium,
        p.holder_id AS policy_holder_id,
        p.end_date AS policy_end_date
    FROM recent_claims s
    LEFT JOIN policies p ON p.id = s.policy_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY policy_holder_id ORDER BY claimed_amount_bucket) AS claimed_amount_bucket_dense_rank,
        SUM(approved_amount) OVER (PARTITION BY TO_DATE(reported_at), policy_holder_id) AS reported_at_to_date_policy_holder_id_sum_approve,
        LAST_VALUE(reported_at) OVER (PARTITION BY adjuster_id ORDER BY policy_premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_reported_at,
        ROUND(approved_amount - AVG(approved_amount) OVER w_reported_at_to_date_policy_h, 2) AS approved_amount_vs_reported_at_to_date_policy_ho,
        DENSE_RANK() OVER (PARTITION BY policy_holder_id ORDER BY claimed_amount_bucket, id) AS claimed_amount_bucket_dense_rank_2,
        DENSE_RANK() OVER (PARTITION BY adjuster_id ORDER BY policy_premium DESC, id) AS policy_premium_dense_rank,
        MAX(claimed_amount_bucket) OVER (PARTITION BY TO_DATE(reported_at), policy_holder_id) AS reported_at_to_date_policy_holder_id_max_claimed,
        MIN(approved_amount) OVER (ORDER BY approved_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_approved_amount_so_far,
        FIRST_VALUE(claimed_amount_bucket) IGNORE NULLS OVER (PARTITION BY adjuster_id ORDER BY policy_premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claimed_amount_bucket,
        ROUND(claimed_amount_bucket - AVG(claimed_amount_bucket) OVER (PARTITION BY TO_DATE(reported_at), policy_holder_id), 2) AS claimed_amount_bucket_vs_reported_at_to_date_pol,
        COUNT(*) OVER (PARTITION BY TO_DATE(reported_at), policy_holder_id) AS rows_in_reported_at_to_date_policy_holder_id,
        LEAD(policy_end_date) OVER (PARTITION BY policy_holder_id ORDER BY claimed_amount_bucket, id) AS next_policy_end_date,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY TO_DATE(reported_at), policy_holder_id), 0), 4) AS approved_amount_share_of_reported_at_to_date_pol
    FROM with_details
    WINDOW w_reported_at_to_date_policy_h AS (PARTITION BY TO_DATE(reported_at), policy_holder_id)
),
top_rows AS (
    -- Top 3 per group
    SELECT *
    FROM sequenced
    WHERE policy_premium_dense_rank <= 3
      AND first_known_claimed_amount_bucket < 42515
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        reported_at,
        policy_end_date,
        claimed_amount_bucket_dense_rank,
        last_reported_at,
        policy_premium_dense_rank,
        reported_at_to_date_policy_holder_id_max_claimed,
        min_approved_amount_so_far,
        first_known_claimed_amount_bucket,
        claimed_amount_bucket_vs_reported_at_to_date_pol,
        rows_in_reported_at_to_date_policy_holder_id,
        next_policy_end_date,
        approved_amount_share_of_reported_at_to_date_pol,
        ROUND(CUME_DIST() OVER w_policy_id_next_policy_end_da, 3) AS reported_at_to_date_policy_holder_id_max_claimed_2,
        SUM(approved_amount) OVER w_policy_premium_dense_rank_po AS policy_premium_dense_rank_policy_end_date_month_,
        ROUND(AVG(approved_amount) OVER (PARTITION BY claimed_amount_bucket_dense_rank_2, adjuster_id ORDER BY policy_premium_dense_rank DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS approved_amount_avg_2_prior
    FROM top_rows
    WINDOW w_policy_id_next_policy_end_da AS (PARTITION BY policy_id, MONTH(next_policy_end_date) ORDER BY reported_at_to_date_policy_holder_id_max_claimed DESC), w_policy_premium_dense_rank_po AS (PARTITION BY policy_premium_dense_rank, MONTH(policy_end_date))
)
-- Final projection
SELECT
    id,
    policy_id,
    claimed_amount_bucket_dense_rank,
    policy_premium_dense_rank,
    first_known_claimed_amount_bucket,
    claimed_amount_bucket_vs_reported_at_to_date_pol,
    next_policy_end_date,
    approved_amount_share_of_reported_at_to_date_pol,
    reported_at_to_date_policy_holder_id_max_claimed_2,
    policy_premium_dense_rank_policy_end_date_month_,
    DATE_FORMAT(last_reported_at, 'MMM yyyy') AS last_reported_at_label
FROM with_windows
WHERE reported_at_to_date_policy_holder_id_max_claimed_2 >= 0.1
ORDER BY reported_at_to_date_policy_holder_id_max_claimed_2 DESC
