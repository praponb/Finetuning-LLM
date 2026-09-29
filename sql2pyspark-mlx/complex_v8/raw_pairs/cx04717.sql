WITH cleaned AS (
    -- Filter claims
    SELECT *
    FROM claims
    WHERE claim_type <> 'delay'
      AND claim_date >= DATE '2024-04-07'
),
with_parent_policy AS (
    -- Join policies
    SELECT
        s.*,
        p.holder_id AS policy_holder_id,
        p.end_date AS policy_end_date,
        p.product AS policy_product,
        a.adjuster_name
    FROM cleaned s
    JOIN policies p ON p.id = s.policy_id AND p.start_date < DATE '2025-08-03'
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(reported_at, 2) OVER (PARTITION BY policy_holder_id ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at,
        SIZE(COLLECT_SET(adjuster_id) OVER w_policy_id_status) AS distinct_adjuster_id_in_policy_id_status,
        LAG(claim_date) OVER (PARTITION BY policy_holder_id ORDER BY approved_amount, id) AS prev_claim_date,
        ROUND(PERCENT_RANK() OVER (ORDER BY reported_at, id), 3) AS reported_at_percent_rank,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY policy_id, status), 0), 4) AS approved_amount_share_of_policy_id_status,
        MAX(claimed_amount) OVER (PARTITION BY policy_id, status) AS policy_id_status_max_claimed_amount,
        ROW_NUMBER() OVER (ORDER BY reported_at, id) AS seq_no,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(reported_at) ORDER BY policy_end_date ASC), 3) AS policy_end_date_cume_dist,
        LAG(approved_amount, 1, 0) OVER (ORDER BY reported_at, id) AS prev_approved_amount
    FROM with_parent_policy
    WINDOW w_policy_id_status AS (PARTITION BY policy_id, status)
)
-- Final projection
SELECT
    id,
    adjuster_id,
    claim_type,
    approved_amount,
    reported_at,
    policy_holder_id,
    policy_end_date,
    policy_product,
    adjuster_name,
    second_reported_at,
    distinct_adjuster_id_in_policy_id_status,
    prev_claim_date,
    reported_at_percent_rank,
    approved_amount_share_of_policy_id_status,
    policy_id_status_max_claimed_amount,
    seq_no,
    policy_end_date_cume_dist,
    prev_approved_amount
FROM sequenced
ORDER BY id DESC, claim_type DESC
