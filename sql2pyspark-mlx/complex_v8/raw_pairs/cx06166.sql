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
        DATE_FORMAT(reported_at, 'yyyy-MM-dd') AS reported_at_label,
        LEAST(approved_amount, claimed_amount) AS least_approved_amount_claimed_amount
    FROM claims
    WHERE (claim_type <> 'theft' OR claimed_amount >= 4698.6)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(claimed_amount) OVER w_policy_id AS policy_id_max_claimed_amount,
        SUM(claimed_amount) OVER (PARTITION BY adjuster_id ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_claimed_amount,
        SUM(claimed_amount) OVER (PARTITION BY policy_id ORDER BY least_approved_amount_claimed_amount ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS claimed_amount_sum_2_prior,
        MAX(approved_amount) OVER (PARTITION BY policy_id) AS policy_id_max_approved_amount,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY policy_id)) AS distinct_claim_type_in_policy_id,
        ROUND(PERCENT_RANK() OVER (ORDER BY least_approved_amount_claimed_amount), 3) AS least_approved_amount_claimed_amount_percent_ran,
        NTH_VALUE(claim_date, 2) OVER (PARTITION BY policy_id ORDER BY least_approved_amount_claimed_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date,
        LAG(claim_date) OVER (ORDER BY least_approved_amount_claimed_amount, id) AS prev_claim_date,
        ROUND(approved_amount - AVG(approved_amount) OVER w_policy_id, 2) AS approved_amount_vs_policy_id_avg,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY policy_id), 2) AS claimed_amount_vs_policy_id_avg,
        ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY least_approved_amount_claimed_amount ASC, id) AS least_approved_amount_claimed_amount_seq,
        LEAD(claim_date) OVER w_all_by_least_approved_amount_2 AS next_claim_date
    FROM valid
    WINDOW w_policy_id AS (PARTITION BY policy_id), w_all_by_least_approved_amount_2 AS (ORDER BY least_approved_amount_claimed_amount, id)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        claimed_amount,
        approved_amount,
        running_claimed_amount,
        policy_id_max_approved_amount,
        distinct_claim_type_in_policy_id,
        least_approved_amount_claimed_amount_percent_ran,
        approved_amount_vs_policy_id_avg,
        claimed_amount_vs_policy_id_avg,
        least_approved_amount_claimed_amount_seq,
        NTH_VALUE(approved_amount, 2) OVER (PARTITION BY status, policy_id ORDER BY claimed_amount_vs_policy_id_avg DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount,
        MAX(claimed_amount_sum_2_prior) OVER (ORDER BY next_claim_date DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS claimed_amount_sum_2_prior_max_1_prior,
        ROUND(CUME_DIST() OVER (ORDER BY next_claim_date DESC, id), 3) AS next_claim_date_cume_dist,
        running_claimed_amount - LAG(running_claimed_amount) OVER (ORDER BY next_claim_date DESC, id) AS running_claimed_amount_change
    FROM windowed
)
-- Final projection
SELECT
    id,
    claimed_amount,
    approved_amount,
    running_claimed_amount,
    policy_id_max_approved_amount,
    least_approved_amount_claimed_amount_percent_ran,
    claimed_amount_vs_policy_id_avg,
    least_approved_amount_claimed_amount_seq,
    second_approved_amount,
    claimed_amount_sum_2_prior_max_1_prior,
    running_claimed_amount_change,
    approved_amount + second_approved_amount AS approved_amount_plus_second_approved_amount
FROM with_trends
ORDER BY least_approved_amount_claimed_amount_percent_ran, id
LIMIT 5
