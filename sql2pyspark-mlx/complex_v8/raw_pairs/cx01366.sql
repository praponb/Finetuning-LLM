WITH recent AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        CONCAT_WS('-', claim_type, status) AS claim_type_status_label,
        LEAST(approved_amount, claimed_amount) AS least_approved_amount_claimed_amount
    FROM claims
    WHERE policy_id IS NOT NULL
      AND approved_amount <= 33690.0
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY YEAR(claim_date) ORDER BY claim_date DESC) AS claim_date_dense_rank,
        MAX(approved_amount) OVER (PARTITION BY status, policy_id) AS status_policy_id_max_approved_amount,
        LEAD(reported_at) OVER w_status_by_least_approved_amo AS next_reported_at,
        ROUND(AVG(approved_amount) OVER (ORDER BY claim_date DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS approved_amount_avg_3_prior,
        NTILE(5) OVER w_status_by_reported_at AS reported_at_quintile,
        NTH_VALUE(reported_at, 2) OVER (PARTITION BY policy_id ORDER BY least_approved_amount_claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at,
        MAX(claimed_amount) OVER (ORDER BY claim_date DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS claimed_amount_max_3_prior,
        LAST_VALUE(reported_at) IGNORE NULLS OVER (PARTITION BY status ORDER BY least_approved_amount_claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_reported_at,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY status, policy_id)) AS distinct_adjuster_id_in_status_policy_id,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY status ORDER BY reported_at, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS claimed_amount_avg_1_prior
    FROM recent
    WINDOW w_status_by_least_approved_amo AS (PARTITION BY status ORDER BY least_approved_amount_claimed_amount DESC, id), w_status_by_reported_at AS (PARTITION BY status ORDER BY reported_at, id), w_status_policy_id AS (PARTITION BY status, policy_id)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        claim_type_status_label,
        claim_date_dense_rank,
        status_policy_id_max_approved_amount,
        claimed_amount_max_3_prior,
        last_known_reported_at,
        claimed_amount_avg_1_prior,
        LAG(claimed_amount_avg_1_prior, 3) OVER (PARTITION BY YEAR(next_reported_at), policy_id ORDER BY claim_date, id) AS prev3_claimed_amount_avg_1_prior,
        MAX(approved_amount) OVER (PARTITION BY reported_at_quintile, claim_date_dense_rank) AS reported_at_quintile_claim_date_dense_rank_max_a,
        RANK() OVER (PARTITION BY YEAR(next_reported_at), policy_id ORDER BY claim_date) AS claim_date_rank
    FROM metrics
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    claimed_amount,
    approved_amount,
    status,
    claim_type_status_label,
    claim_date_dense_rank,
    status_policy_id_max_approved_amount,
    claimed_amount_max_3_prior,
    last_known_reported_at,
    claimed_amount_avg_1_prior,
    reported_at_quintile_claim_date_dense_rank_max_a,
    claim_date_rank
FROM with_trends
WHERE claim_type_status_label IS NOT NULL
ORDER BY claim_date_dense_rank DESC, claim_date_rank
