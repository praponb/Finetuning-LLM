WITH scoped AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        reported_at,
        ABS(claimed_amount - 28426.9) AS claimed_amount_distance,
        GREATEST(approved_amount, claimed_amount) AS greatest_approved_amount_claimed_amount
    FROM claims
    WHERE (approved_amount < 17759.3 OR HOUR(reported_at) BETWEEN 8 AND 14)
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        claim_date,
        reported_at,
        claimed_amount_distance,
        ROUND(claimed_amount_distance / NULLIF(approved_amount, 0), 3) AS claimed_amount_distance_per_approved_amount
    FROM scoped
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        claim_date,
        reported_at,
        DENSE_RANK() OVER (PARTITION BY MONTH(claim_date) ORDER BY reported_at DESC, id) AS reported_at_dense_rank,
        ROUND(CUME_DIST() OVER w_reported_at_month_by_claim_d, 3) AS claim_date_cume_dist,
        ROUND(claimed_amount_distance / NULLIF(SUM(claimed_amount_distance) OVER (PARTITION BY TO_DATE(reported_at)), 0), 4) AS claimed_amount_distance_share_of_reported_at_to_,
        COUNT(*) OVER w_reported_at_to_date AS rows_in_reported_at_to_date,
        DENSE_RANK() OVER w_claim_date_month_by_reported_2 AS reported_at_dense_rank_2
    FROM labeled
    WINDOW w_reported_at_month_by_claim_d AS (PARTITION BY MONTH(reported_at) ORDER BY claim_date NULLS LAST, id), w_reported_at_to_date AS (PARTITION BY TO_DATE(reported_at)), w_claim_date_month_by_reported_2 AS (PARTITION BY MONTH(claim_date) ORDER BY reported_at DESC)
)
-- Final projection
SELECT
    id,
    reported_at_dense_rank,
    claim_date_cume_dist,
    claimed_amount_distance_share_of_reported_at_to_,
    rows_in_reported_at_to_date,
    reported_at_dense_rank_2
FROM with_windows
ORDER BY claim_date_cume_dist, claimed_amount_distance_share_of_reported_at_to_ DESC NULLS FIRST
