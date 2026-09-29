WITH filtered AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status
    FROM claims
    WHERE policy_id IS NOT NULL
),
joined_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.approved_amount,
        s.status,
        p.holder_id AS policy_holder_id,
        p.start_date AS policy_start_date
    FROM filtered s
    LEFT JOIN policies p ON p.id = s.policy_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY approved_amount DESC, id) AS seq_no,
        ROW_NUMBER() OVER w_claim_type_policy_start_date AS policy_start_date_seq,
        ROUND(CUME_DIST() OVER w_claim_type_policy_start_date_2, 3) AS policy_start_date_cume_dist,
        COUNT(*) OVER (PARTITION BY claim_type, DATE_TRUNC('MONTH', policy_start_date) ORDER BY policy_start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        DENSE_RANK() OVER (PARTITION BY claim_type, DATE_TRUNC('MONTH', policy_start_date) ORDER BY policy_start_date ASC) AS policy_start_date_dense_rank,
        DENSE_RANK() OVER w_all_by_policy_start_date_tie AS policy_start_date_dense_rank_2,
        ROUND(approved_amount - AVG(approved_amount) OVER w_claim_type, 2) AS approved_amount_vs_claim_type_avg,
        RANK() OVER (ORDER BY policy_start_date DESC) AS policy_start_date_rank,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY DATE_TRUNC('MONTH', policy_start_date), policy_holder_id)) AS distinct_adjuster_id_in_policy_start_date_month_,
        RANK() OVER (PARTITION BY status ORDER BY policy_start_date) AS policy_start_date_rank_2,
        MAX(approved_amount) OVER (ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_approved_amount_so_far
    FROM joined_policy
    WINDOW w_claim_type_policy_start_date AS (PARTITION BY claim_type, DATE_TRUNC('MONTH', policy_start_date) ORDER BY policy_start_date ASC, id), w_claim_type_policy_start_date_2 AS (PARTITION BY claim_type, DATE_TRUNC('MONTH', policy_start_date) ORDER BY policy_start_date ASC), w_all_by_policy_start_date_tie AS (ORDER BY policy_start_date DESC), w_claim_type AS (PARTITION BY claim_type)
)
SELECT
    id,
    claim_type,
    approved_amount,
    status,
    policy_holder_id,
    policy_start_date,
    seq_no,
    policy_start_date_seq,
    policy_start_date_cume_dist,
    running_count,
    policy_start_date_dense_rank,
    approved_amount_vs_claim_type_avg,
    policy_start_date_rank_2,
    max_approved_amount_so_far,
    FLOOR(seq_no / 2) * 2 AS seq_no_bucket
FROM with_trends
WHERE policy_start_date_seq BETWEEN 4 AND 9
ORDER BY status NULLS LAST, id
LIMIT 10
