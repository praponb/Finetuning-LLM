WITH prepared AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at
    FROM claims
    WHERE claim_date >= DATE '2024-06-13'
),
combined_details_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.status,
        p.holder_id AS policy_holder_id,
        p.product AS policy_product
    FROM prepared s
    LEFT JOIN policies p ON p.id = s.policy_id AND p.status = 'EXPIRED'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        policy_holder_id,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER w_status, 0), 4) AS claimed_amount_share_of_status,
        ROUND(claimed_amount - AVG(claimed_amount) OVER w_status, 2) AS claimed_amount_vs_status_avg,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY policy_holder_id), 0), 4) AS claimed_amount_share_of_policy_holder_id,
        COUNT(*) OVER w_policy_holder_id AS rows_in_policy_holder_id,
        SUM(claimed_amount) OVER (PARTITION BY status) AS status_sum_claimed_amount,
        ROUND(CUME_DIST() OVER (ORDER BY claim_date), 3) AS claim_date_cume_dist,
        LEAD(claim_date) OVER w_all_by_claim_date AS next_claim_date,
        DENSE_RANK() OVER (ORDER BY claim_date) AS claim_date_dense_rank,
        SUM(claimed_amount) OVER w_policy_holder_id AS policy_holder_id_sum_claimed_amount,
        MIN(claimed_amount) OVER (PARTITION BY policy_holder_id) AS policy_holder_id_min_claimed_amount
    FROM combined_details_policy
    WINDOW w_status AS (PARTITION BY status), w_policy_holder_id AS (PARTITION BY policy_holder_id), w_all_by_claim_date AS (ORDER BY claim_date, id)
),
windowed_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY policy_holder_id, status ORDER BY status_sum_claimed_amount DESC), 3) AS status_sum_claimed_amount_percent_rank,
        MIN(claimed_amount_vs_status_avg) OVER (PARTITION BY policy_id, DATE_TRUNC('MONTH', next_claim_date) ORDER BY status_sum_claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_claimed_amount_vs_status_avg_so_far,
        SUM(claimed_amount) OVER (PARTITION BY policy_id) AS policy_id_sum_claimed_amount
    FROM windowed
)
-- Final projection
SELECT
    id,
    claim_type,
    claimed_amount,
    status,
    policy_holder_id,
    claimed_amount_share_of_status,
    claimed_amount_vs_status_avg,
    claimed_amount_share_of_policy_holder_id,
    rows_in_policy_holder_id,
    status_sum_claimed_amount,
    claim_date_cume_dist,
    next_claim_date,
    policy_holder_id_sum_claimed_amount,
    policy_holder_id_min_claimed_amount,
    status_sum_claimed_amount_percent_rank,
    policy_id_sum_claimed_amount,
    CONCAT(claim_type, '#', CAST(id AS STRING)) AS claim_type_ref
FROM windowed_2
ORDER BY claimed_amount_vs_status_avg DESC, next_claim_date DESC, id
LIMIT 10
