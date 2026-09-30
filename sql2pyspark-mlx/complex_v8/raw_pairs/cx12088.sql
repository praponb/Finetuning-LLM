WITH valid_claims AS (
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
        reported_at
    FROM claims
    WHERE claimed_amount >= 20436.5
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        claim_date,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        ROW_NUMBER() OVER (PARTITION BY claim_type, adjuster_id ORDER BY claimed_amount DESC, id) AS seq_no,
        LAST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY TO_DATE(reported_at), adjuster_id ORDER BY claimed_amount DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claim_date,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY adjuster_id, policy_id), 0), 4) AS claimed_amount_share_of_adjuster_id_policy_id,
        COUNT(*) OVER (PARTITION BY status, adjuster_id) AS rows_in_status_adjuster_id,
        claimed_amount - LAG(claimed_amount) OVER (PARTITION BY TO_DATE(reported_at), adjuster_id ORDER BY claimed_amount DESC NULLS FIRST, id) AS claimed_amount_change,
        DENSE_RANK() OVER (PARTITION BY claim_type, adjuster_id ORDER BY claimed_amount DESC) AS claimed_amount_dense_rank,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY adjuster_id, policy_id), 0), 4) AS approved_amount_share_of_adjuster_id_policy_id,
        RANK() OVER w_status_reported_at_to_date_b AS reported_at_rank,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY adjuster_id, policy_id), 2) AS approved_amount_vs_adjuster_id_policy_id_avg,
        SUM(claimed_amount) OVER (PARTITION BY claim_type, adjuster_id ORDER BY claimed_amount DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS claimed_amount_sum_4,
        ROUND(AVG(approved_amount) OVER (PARTITION BY policy_id, status ORDER BY reported_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS approved_amount_avg_4
    FROM valid_claims
    WINDOW w_status_reported_at_to_date_b AS (PARTITION BY status, TO_DATE(reported_at) ORDER BY reported_at DESC, id)
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    claimed_amount,
    approved_amount,
    status,
    reported_at,
    seq_no,
    last_known_claim_date,
    rows_in_status_adjuster_id,
    claimed_amount_change,
    claimed_amount_dense_rank,
    reported_at_rank,
    claimed_amount_sum_4,
    approved_amount_avg_4,
    DATE_TRUNC('YEAR', claim_date) AS claim_date_year
FROM sequenced
