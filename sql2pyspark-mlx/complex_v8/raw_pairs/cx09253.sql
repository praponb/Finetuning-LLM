WITH valid_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount
    FROM claims
    WHERE claim_type = 'collision'
      AND status = 'OPEN'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        ROUND(CUME_DIST() OVER (PARTITION BY policy_id ORDER BY approved_amount), 3) AS approved_amount_cume_dist,
        RANK() OVER (ORDER BY claim_date ASC) AS claim_date_rank,
        LAG(claimed_amount) OVER (PARTITION BY MONTH(claim_date), policy_id ORDER BY approved_amount DESC NULLS FIRST, id) AS prev_claimed_amount,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY policy_id, adjuster_id), 2) AS policy_id_adjuster_id_avg_claimed_amount,
        MAX(claimed_amount) OVER (ORDER BY approved_amount, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS claimed_amount_max_3,
        LAST_VALUE(approved_amount) IGNORE NULLS OVER (PARTITION BY MONTH(claim_date) ORDER BY claimed_amount ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_approved_amount,
        LEAD(claimed_amount) OVER (PARTITION BY MONTH(claim_date) ORDER BY claimed_amount ASC NULLS LAST, id) AS next_claimed_amount,
        COUNT(*) OVER w_policy_id AS rows_in_policy_id,
        ROUND(AVG(approved_amount) OVER (PARTITION BY policy_id, DATE_TRUNC('MONTH', claim_date)), 2) AS policy_id_claim_date_month_avg_approved_amount,
        LAG(claim_date) OVER (PARTITION BY MONTH(claim_date) ORDER BY claimed_amount ASC NULLS LAST, id) AS prev_claim_date,
        NTH_VALUE(approved_amount, 2) OVER (PARTITION BY adjuster_id ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount,
        ROUND(AVG(claimed_amount) OVER (ORDER BY approved_amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS claimed_amount_avg_4,
        ROUND(claimed_amount - AVG(claimed_amount) OVER w_policy_id_claim_date_month, 2) AS claimed_amount_vs_policy_id_claim_date_month_avg,
        ROUND(CUME_DIST() OVER w_claim_date_month_policy_id_b_2, 3) AS approved_amount_cume_dist_2,
        DENSE_RANK() OVER w_all_by_claim_date AS claim_date_dense_rank
    FROM valid_claims
    WINDOW w_policy_id AS (PARTITION BY policy_id), w_policy_id_claim_date_month AS (PARTITION BY policy_id, DATE_TRUNC('MONTH', claim_date)), w_claim_date_month_policy_id_b_2 AS (PARTITION BY MONTH(claim_date), policy_id ORDER BY approved_amount DESC NULLS FIRST), w_all_by_claim_date AS (ORDER BY claim_date ASC, id)
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN claimed_amount_max_3 >= 97535.0 THEN 'large' WHEN claimed_amount_max_3 >= 37575.0 THEN 'medium' ELSE 'small' END AS claimed_amount_max_3_tier
    FROM ranked
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    approved_amount,
    approved_amount_cume_dist,
    claim_date_rank,
    prev_claimed_amount,
    policy_id_adjuster_id_avg_claimed_amount,
    claimed_amount_max_3,
    last_known_approved_amount,
    prev_claim_date,
    second_approved_amount,
    claimed_amount_avg_4,
    claimed_amount_vs_policy_id_claim_date_month_avg,
    approved_amount_cume_dist_2,
    claimed_amount_max_3_tier,
    DATEDIFF(DATE '2025-06-30', claim_date) AS days_since_claim_date
FROM annotated
WHERE adjuster_id >= 3
ORDER BY prev_claim_date
