WITH valid AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        reported_at,
        LENGTH(TRIM(claim_type)) AS claim_type_len,
        CASE WHEN status IN ('OPEN', 'PAID') THEN 'group_a' WHEN status = 'APPROVED' THEN 'group_b' ELSE 'other' END AS status_group
    FROM claims
    WHERE MONTH(reported_at) IN (2, 3)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        claim_date,
        approved_amount,
        reported_at,
        claim_type_len,
        status_group,
        LAG(claim_date) OVER (ORDER BY approved_amount ASC, id) AS prev_claim_date,
        MIN(approved_amount) OVER (ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_approved_amount_so_far,
        ROUND(claim_type_len - AVG(claim_type_len) OVER (PARTITION BY status_group), 2) AS claim_type_len_vs_status_group_avg,
        NTILE(3) OVER w_policy_id_by_claim_type_len AS claim_type_len_tercile,
        LEAD(approved_amount, 1, 0) OVER (PARTITION BY policy_id ORDER BY claim_type_len DESC, id) AS next_approved_amount,
        MAX(claimed_amount) OVER w_status_group AS status_group_max_claimed_amount,
        MIN(claimed_amount) OVER (PARTITION BY adjuster_id, status_group) AS adjuster_id_status_group_min_claimed_amount,
        LEAD(reported_at, 3) OVER (ORDER BY reported_at DESC, id) AS next3_reported_at
    FROM valid
    WINDOW w_policy_id_by_claim_type_len AS (PARTITION BY policy_id ORDER BY claim_type_len DESC, id), w_status_group AS (PARTITION BY status_group)
)
SELECT
    id,
    policy_id,
    claim_date,
    approved_amount,
    reported_at,
    status_group,
    prev_claim_date,
    claim_type_len_vs_status_group_avg,
    claim_type_len_tercile,
    status_group_max_claimed_amount,
    adjuster_id_status_group_min_claimed_amount,
    next3_reported_at
FROM sequenced
ORDER BY id DESC, status_group_max_claimed_amount DESC
LIMIT 5
