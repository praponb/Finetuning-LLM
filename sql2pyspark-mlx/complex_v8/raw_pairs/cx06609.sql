WITH filtered_claims AS (
    -- Filter claims
    SELECT *
    FROM claims
    WHERE status IN ('PAID', 'APPROVED')
      AND policy_id >= 13
),
metrics AS (
    -- Previous / next values per key
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
        ROUND(AVG(claimed_amount) OVER (PARTITION BY policy_id ORDER BY claimed_amount, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS claimed_amount_avg_6_prior,
        DENSE_RANK() OVER (ORDER BY approved_amount NULLS LAST) AS approved_amount_dense_rank,
        LAG(claimed_amount) OVER (ORDER BY reported_at ASC, id) AS prev_claimed_amount,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY policy_id)) AS distinct_status_in_policy_id,
        MAX(approved_amount) OVER (PARTITION BY policy_id ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_approved_amount_so_far
    FROM filtered_claims
),
without_matches AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.status NOT IN ('APPROVED', 'PAID')
              AND c.claim_date BETWEEN DATE '2024-03-16' AND DATE '2025-04-20'
        )
      AND s.approved_amount_dense_rank > 3
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(prev_claimed_amount) OVER (PARTITION BY policy_id ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev_claimed_amount,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY status, claim_type)) AS distinct_adjuster_id_in_status_claim_type,
        ROUND(PERCENT_RANK() OVER (PARTITION BY policy_id ORDER BY claim_date DESC, id), 3) AS claim_date_percent_rank,
        RANK() OVER w_policy_id_by_claim_date_ties AS claim_date_rank
    FROM without_matches
    WINDOW w_policy_id_by_claim_date_ties AS (PARTITION BY policy_id ORDER BY claim_date DESC)
),
joined AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.claimed_amount_avg_6_prior,
        s.approved_amount_dense_rank,
        s.prev_claimed_amount,
        s.max_approved_amount_so_far,
        s.running_prev_claimed_amount,
        s.distinct_adjuster_id_in_status_claim_type,
        s.claim_date_rank,
        p.end_date AS policy_end_date,
        p.product AS policy_product,
        p.holder_id AS policy_holder_id
    FROM with_trends s
    JOIN policies p ON p.id = s.policy_id
),
latest_per_key AS (
    -- Keep the latest row per claim_type, status
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
        claimed_amount_avg_6_prior,
        approved_amount_dense_rank,
        prev_claimed_amount,
        max_approved_amount_so_far,
        running_prev_claimed_amount,
        distinct_adjuster_id_in_status_claim_type,
        claim_date_rank,
        policy_end_date,
        policy_product,
        policy_holder_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_type, status ORDER BY policy_end_date DESC, id DESC) AS recency_rank
        FROM joined
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claim_type,
    approved_amount,
    status,
    reported_at,
    claimed_amount_avg_6_prior,
    approved_amount_dense_rank,
    prev_claimed_amount,
    max_approved_amount_so_far,
    distinct_adjuster_id_in_status_claim_type,
    claim_date_rank,
    policy_end_date,
    policy_product,
    DATEDIFF(claim_date, policy_end_date) AS days_policy_end_date_to_claim_date
FROM latest_per_key
ORDER BY reported_at
