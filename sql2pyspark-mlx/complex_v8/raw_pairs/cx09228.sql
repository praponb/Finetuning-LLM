WITH valid AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        reported_at
    FROM claims
),
with_details AS (
    -- Join policies
    SELECT s.*, p.holder_id AS policy_holder_id, p.end_date AS policy_end_date
    FROM valid s
    JOIN policies p ON p.id = s.policy_id AND p.status <> 'CANCELLED'
    WHERE p.end_date < DATE '2026-06-18'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(policy_end_date) OVER (PARTITION BY policy_id ORDER BY claimed_amount DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_policy_end_date,
        SUM(claimed_amount) OVER (ORDER BY reported_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_claimed_amount,
        ROUND(PERCENT_RANK() OVER w_all_by_reported_at_ties, 3) AS reported_at_percent_rank,
        MIN(claimed_amount) OVER (ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_claimed_amount_so_far,
        DATEDIFF(claim_date, LAG(claim_date) OVER w_policy_id_by_claimed_amount_2) AS days_since_prev_claim_date,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY status, policy_holder_id), 2) AS claimed_amount_vs_status_policy_holder_id_avg,
        MIN(claimed_amount) OVER (ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_claimed_amount_so_far_2,
        COUNT(*) OVER (PARTITION BY policy_holder_id) AS rows_in_policy_holder_id,
        ROW_NUMBER() OVER (ORDER BY reported_at ASC, id) AS seq_no,
        LEAD(policy_end_date) OVER (ORDER BY claim_date, id) AS next_policy_end_date
    FROM with_details
    WINDOW w_all_by_reported_at_ties AS (ORDER BY reported_at ASC), w_policy_id_by_claimed_amount_2 AS (PARTITION BY policy_id ORDER BY claimed_amount DESC NULLS FIRST, id), w_status_policy_holder_id AS (PARTITION BY status, policy_holder_id)
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref,
        CONCAT(status, '#', CAST(seq_no AS STRING)) AS status_ref_2
    FROM windowed
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(rows_in_policy_holder_id) OVER (PARTITION BY status, policy_id ORDER BY last_policy_end_date DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS rows_in_policy_holder_id_sum_2_prior,
        SUM(claimed_amount) OVER (PARTITION BY status, policy_id ORDER BY last_policy_end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_claimed_amount_2,
        rows_in_policy_holder_id - LAG(rows_in_policy_holder_id) OVER (PARTITION BY seq_no ORDER BY reported_at, id) AS rows_in_policy_holder_id_change,
        ROUND(AVG(running_claimed_amount) OVER (PARTITION BY adjuster_id, seq_no), 2) AS adjuster_id_seq_no_avg_running_claimed_amount,
        SIZE(COLLECT_SET(policy_id) OVER w_adjuster_id_seq_no) AS distinct_policy_id_in_adjuster_id_seq_no
    FROM derived
    WINDOW w_adjuster_id_seq_no AS (PARTITION BY adjuster_id, seq_no)
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claimed_amount,
    status,
    reported_at,
    policy_end_date,
    last_policy_end_date,
    running_claimed_amount,
    reported_at_percent_rank,
    min_claimed_amount_so_far,
    days_since_prev_claim_date,
    claimed_amount_vs_status_policy_holder_id_avg,
    min_claimed_amount_so_far_2,
    rows_in_policy_holder_id,
    seq_no,
    next_policy_end_date,
    status_ref,
    status_ref_2,
    rows_in_policy_holder_id_sum_2_prior,
    running_claimed_amount_2,
    rows_in_policy_holder_id_change,
    distinct_policy_id_in_adjuster_id_seq_no,
    FLOOR(running_claimed_amount / 100000) * 100000 AS running_claimed_amount_bucket
FROM with_trends
ORDER BY min_claimed_amount_so_far_2, status_ref DESC
