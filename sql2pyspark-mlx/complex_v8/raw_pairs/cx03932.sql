WITH prepared AS (
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
        reported_at
    FROM claims
    WHERE adjuster_id <= 7
      AND MONTH(reported_at) IN (1, 2)
),
with_details_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.reported_at,
        p.premium AS policy_premium,
        p.holder_id AS policy_holder_id,
        p.product AS policy_product
    FROM prepared s
    JOIN policies p ON p.id = s.policy_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(approved_amount, 2) OVER (PARTITION BY adjuster_id ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount,
        DATEDIFF(claim_date, LAG(claim_date) OVER (PARTITION BY adjuster_id ORDER BY approved_amount DESC, id)) AS days_since_prev_claim_date,
        LAG(reported_at, 2) OVER (PARTITION BY policy_id, status ORDER BY approved_amount ASC, id) AS prev2_reported_at,
        NTILE(3) OVER w_adjuster_id_by_approved_amou_2 AS approved_amount_tercile,
        SUM(policy_premium) OVER (PARTITION BY policy_product, policy_holder_id) AS policy_product_policy_holder_id_sum_policy_premi,
        ROUND(policy_premium / NULLIF(SUM(policy_premium) OVER (PARTITION BY policy_product, policy_holder_id), 0), 4) AS policy_premium_share_of_policy_product_policy_ho,
        LAST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY policy_id, policy_holder_id ORDER BY claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claim_date,
        DENSE_RANK() OVER (PARTITION BY claim_type ORDER BY reported_at, id) AS reported_at_dense_rank,
        LEAD(approved_amount) OVER w_adjuster_id_by_approved_amou_2 AS next_approved_amount,
        FIRST_VALUE(policy_premium) OVER (PARTITION BY adjuster_id ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_policy_premium
    FROM with_details_policy
    WINDOW w_adjuster_id_by_approved_amou_2 AS (PARTITION BY adjuster_id ORDER BY approved_amount DESC, id)
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        TRUNC(last_known_claim_date, 'MM') AS last_known_claim_date_month_start
    FROM metrics
),
grouped AS (
    -- Group-level totals
    SELECT
        status,
        PERCENTILE_APPROX(next_approved_amount, 0.5) AS median_next_approved_amount,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(days_since_prev_claim_date) AS total_days_since_prev_claim_date,
        SUM(policy_premium_share_of_policy_product_policy_ho) AS total_policy_premium_share_of_policy_product_pol
    FROM derived
    GROUP BY status
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(median_next_approved_amount) OVER (PARTITION BY distinct_ids ORDER BY total_days_since_prev_claim_date DESC, status ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS median_next_approved_amount_sum_2_prior,
        SUM(distinct_ids) OVER (PARTITION BY distinct_ids ORDER BY total_days_since_prev_claim_date DESC, status ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS distinct_ids_sum_3,
        SUM(median_next_approved_amount) OVER (PARTITION BY status, distinct_ids) AS status_distinct_ids_sum_median_next_approved_amo
    FROM grouped
)
-- Final projection
SELECT DISTINCT status, distinct_ids
FROM windowed
ORDER BY status DESC, distinct_ids DESC
