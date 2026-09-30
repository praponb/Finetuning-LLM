WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        LOWER(TRIM(status)) AS status_clean
    FROM claims
    WHERE YEAR(claim_date) = 2025
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        status,
        reported_at,
        status_clean,
        MAX(claimed_amount) OVER (PARTITION BY policy_id, claim_type ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_claimed_amount_so_far,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY policy_id, status), 2) AS policy_id_status_avg_claimed_amount,
        ROUND(AVG(claimed_amount) OVER w_status_adjuster_id, 2) AS status_adjuster_id_avg_claimed_amount,
        ROUND(claimed_amount - AVG(claimed_amount) OVER w_status_adjuster_id, 2) AS claimed_amount_vs_status_adjuster_id_avg,
        SUM(claimed_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), claim_type ORDER BY claim_date, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS claimed_amount_sum_2,
        ROW_NUMBER() OVER (PARTITION BY policy_id, claim_type ORDER BY reported_at, id) AS rn,
        SUM(claimed_amount) OVER w_policy_id_status AS policy_id_status_sum_claimed_amount,
        ROW_NUMBER() OVER w_policy_id_by_claim_date AS claim_date_seq,
        SUM(claimed_amount) OVER (PARTITION BY claim_type, policy_id ORDER BY claimed_amount ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS claimed_amount_sum_1_prior,
        ROUND(claimed_amount - AVG(claimed_amount) OVER w_claim_type_status, 2) AS claimed_amount_vs_claim_type_status_avg,
        SIZE(COLLECT_SET(policy_id) OVER (PARTITION BY status, adjuster_id)) AS distinct_policy_id_in_status_adjuster_id,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY claim_type, status), 0), 4) AS claimed_amount_share_of_claim_type_status,
        NTILE(3) OVER w_claim_type_by_claimed_amount AS claimed_amount_tercile,
        ROUND(CUME_DIST() OVER (PARTITION BY claim_type, policy_id ORDER BY claimed_amount ASC, id), 3) AS claimed_amount_cume_dist,
        MAX(claimed_amount) OVER (PARTITION BY policy_id, claim_type ORDER BY reported_at, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS claimed_amount_max_1_prior
    FROM recent
    WINDOW w_status_adjuster_id AS (PARTITION BY status, adjuster_id), w_policy_id_status AS (PARTITION BY policy_id, status), w_policy_id_by_claim_date AS (PARTITION BY policy_id ORDER BY claim_date, id), w_claim_type_status AS (PARTITION BY claim_type, status), w_claim_type_by_claimed_amount AS (PARTITION BY claim_type ORDER BY claimed_amount, id)
),
sequenced_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY rn, adjuster_id ORDER BY claimed_amount_vs_claim_type_status_avg, id) AS claimed_amount_vs_claim_type_status_avg_rank,
        ROUND(claimed_amount_max_1_prior / NULLIF(SUM(claimed_amount_max_1_prior) OVER (PARTITION BY adjuster_id), 0), 4) AS claimed_amount_max_1_prior_share_of_adjuster_id,
        ROUND(claimed_amount_tercile / NULLIF(SUM(claimed_amount_tercile) OVER w_adjuster_id, 0), 4) AS claimed_amount_tercile_share_of_adjuster_id,
        ROUND(AVG(rn) OVER (PARTITION BY adjuster_id), 2) AS adjuster_id_avg_rn
    FROM sequenced
    WINDOW w_adjuster_id AS (PARTITION BY adjuster_id)
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_type,
    reported_at,
    policy_id_status_avg_claimed_amount,
    status_adjuster_id_avg_claimed_amount,
    claimed_amount_vs_status_adjuster_id_avg,
    rn,
    policy_id_status_sum_claimed_amount,
    claim_date_seq,
    claimed_amount_vs_claim_type_status_avg,
    claimed_amount_share_of_claim_type_status,
    claimed_amount_tercile,
    claimed_amount_cume_dist,
    claimed_amount_vs_claim_type_status_avg_rank,
    claimed_amount_max_1_prior_share_of_adjuster_id,
    claimed_amount_tercile_share_of_adjuster_id,
    DATEDIFF(DATE '2025-06-30', reported_at) AS days_since_reported_at,
    ROUND(claimed_amount_tercile / NULLIF(claimed_amount_sum_2, 0), 3) AS claimed_amount_tercile_per_claimed_amount_sum_2
FROM sequenced_2
ORDER BY claimed_amount_tercile DESC, claimed_amount_tercile_share_of_adjuster_id DESC, id
LIMIT 10
