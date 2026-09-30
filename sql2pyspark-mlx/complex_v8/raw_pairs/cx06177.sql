WITH base AS (
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
        CASE WHEN approved_amount >= 23120.7 THEN 'HIGH' WHEN approved_amount >= 0.0 THEN 'MEDIUM' ELSE 'LOW' END AS approved_amount_tier
    FROM claims
    WHERE reported_at < TIMESTAMP '2025-03-08 14:00:00'
      AND adjuster_id IS NOT NULL
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        FIRST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY claim_type ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claim_date,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY TO_DATE(reported_at)), 2) AS claimed_amount_vs_reported_at_to_date_avg,
        COUNT(*) OVER w_reported_at_to_date AS rows_in_reported_at_to_date,
        RANK() OVER (PARTITION BY claim_type ORDER BY reported_at DESC, id) AS reported_at_rank,
        NTH_VALUE(approved_amount, 2) OVER (ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount,
        LAG(approved_amount, 2) OVER w_all_by_claim_date AS prev2_approved_amount,
        ROUND(PERCENT_RANK() OVER w_claim_type_by_reported_at, 3) AS reported_at_percent_rank,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY TO_DATE(reported_at))) AS distinct_status_in_reported_at_to_date,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER w_reported_at_to_date, 0), 4) AS claimed_amount_share_of_reported_at_to_date,
        LAG(reported_at, 3) OVER (PARTITION BY claim_type ORDER BY reported_at DESC, id) AS prev3_reported_at,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER w_reported_at_to_date, 0), 4) AS approved_amount_share_of_reported_at_to_date,
        DENSE_RANK() OVER (ORDER BY claim_date DESC) AS claim_date_dense_rank
    FROM base
    WINDOW w_reported_at_to_date AS (PARTITION BY TO_DATE(reported_at)), w_all_by_claim_date AS (ORDER BY claim_date DESC, id), w_claim_type_by_reported_at AS (PARTITION BY claim_type ORDER BY reported_at DESC, id)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY claimed_amount DESC NULLS FIRST, id), 3) AS claimed_amount_cume_dist,
        ROW_NUMBER() OVER w_all_by_reported_at_percent_r AS reported_at_percent_rank_seq,
        LAG(prev3_reported_at) OVER w_reported_at_rank_by_claimed AS prev_prev3_reported_at
    FROM metrics
    WINDOW w_all_by_reported_at_percent_r AS (ORDER BY reported_at_percent_rank, id), w_reported_at_rank_by_claimed AS (PARTITION BY reported_at_rank ORDER BY claimed_amount DESC, id)
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    claim_type,
    approved_amount,
    first_known_claim_date,
    rows_in_reported_at_to_date,
    prev2_approved_amount,
    reported_at_percent_rank,
    claimed_amount_share_of_reported_at_to_date,
    prev3_reported_at,
    approved_amount_share_of_reported_at_to_date,
    claim_date_dense_rank,
    claimed_amount_cume_dist,
    prev_prev3_reported_at,
    TO_DATE(prev_prev3_reported_at) AS prev_prev3_reported_at_date,
    ABS(prev2_approved_amount - 14993.0) AS prev2_approved_amount_distance
FROM windowed
ORDER BY reported_at_percent_rank DESC
