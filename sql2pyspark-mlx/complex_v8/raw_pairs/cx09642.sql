WITH prepared_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        DATEDIFF(DATE '2026-01-01', reported_at) AS days_since_reported_at,
        ABS(claimed_amount - 29985.9) AS claimed_amount_distance
    FROM claims
    WHERE status = 'REJECTED'
      AND claimed_amount > 13635.1
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        reported_at,
        days_since_reported_at,
        claimed_amount_distance,
        COUNT(*) OVER (PARTITION BY YEAR(claim_date)) AS rows_in_claim_date_year,
        ROUND(claimed_amount_distance / NULLIF(SUM(claimed_amount_distance) OVER (PARTITION BY YEAR(claim_date)), 0), 4) AS claimed_amount_distance_share_of_claim_date_year,
        ROUND(days_since_reported_at - AVG(days_since_reported_at) OVER (PARTITION BY policy_id, TO_DATE(reported_at)), 2) AS days_since_reported_at_vs_policy_id_reported_at_,
        DENSE_RANK() OVER (ORDER BY claimed_amount DESC) AS claimed_amount_dense_rank,
        ROUND(PERCENT_RANK() OVER w_claim_type_adjuster_id_by_cl, 3) AS claimed_amount_percent_rank,
        NTH_VALUE(reported_at, 2) OVER (PARTITION BY claim_type, adjuster_id ORDER BY claimed_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY policy_id, TO_DATE(reported_at)), 2) AS claimed_amount_vs_policy_id_reported_at_to_date_,
        DATEDIFF(claim_date, LAG(claim_date) OVER (PARTITION BY adjuster_id, policy_id ORDER BY days_since_reported_at, id)) AS days_since_prev_claim_date
    FROM prepared_claims
    WINDOW w_claim_type_adjuster_id_by_cl AS (PARTITION BY claim_type, adjuster_id ORDER BY claimed_amount ASC)
),
first_rows AS (
    -- First row per group
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        reported_at,
        days_since_reported_at,
        claimed_amount_distance,
        rows_in_claim_date_year,
        claimed_amount_distance_share_of_claim_date_year,
        days_since_reported_at_vs_policy_id_reported_at_,
        claimed_amount_percent_rank,
        second_reported_at,
        claimed_amount_vs_policy_id_reported_at_to_date_,
        days_since_prev_claim_date
    FROM ranked
    WHERE claimed_amount_dense_rank = 1
)
-- Final projection
SELECT
    id,
    adjuster_id,
    reported_at,
    days_since_reported_at,
    claimed_amount_distance,
    rows_in_claim_date_year,
    claimed_amount_distance_share_of_claim_date_year,
    days_since_reported_at_vs_policy_id_reported_at_,
    second_reported_at,
    claimed_amount_vs_policy_id_reported_at_to_date_,
    days_since_prev_claim_date,
    FLOOR(days_since_reported_at / 100) * 100 AS days_since_reported_at_bucket,
    LAST_DAY(claim_date) AS claim_date_month_end
FROM first_rows
WHERE adjuster_id IS NOT NULL
ORDER BY reported_at
