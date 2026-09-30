WITH cleaned AS (
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
        QUARTER(claim_date) AS claim_date_quarter,
        ROUND(claimed_amount * 1.5, 2) AS claimed_amount_adj
    FROM claims
    WHERE claim_date < DATE '2025-04-14'
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(approved_amount) OVER (PARTITION BY adjuster_id), 2) AS adjuster_id_avg_approved_amount,
        LEAD(claim_date, 2) OVER (ORDER BY approved_amount ASC, id) AS next2_claim_date,
        ROW_NUMBER() OVER w_status_by_claim_date_quarter AS rn,
        FIRST_VALUE(claim_date) OVER (ORDER BY approved_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claim_date,
        LEAD(claim_date) OVER w_status_by_claim_date_quarter AS next_claim_date,
        NTH_VALUE(approved_amount, 2) OVER (PARTITION BY status ORDER BY claim_date_quarter, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount,
        NTH_VALUE(claimed_amount, 2) OVER (ORDER BY approved_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claimed_amount,
        MIN(claimed_amount_adj) OVER (PARTITION BY status, claim_type) AS status_claim_type_min_claimed_amount_adj,
        ROW_NUMBER() OVER (ORDER BY approved_amount ASC, id) AS seq_no
    FROM cleaned
    WINDOW w_status_by_claim_date_quarter AS (PARTITION BY status ORDER BY claim_date_quarter, id)
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type,
    status,
    claim_date_quarter,
    claimed_amount_adj,
    adjuster_id_avg_approved_amount,
    next2_claim_date,
    rn,
    first_claim_date,
    next_claim_date,
    second_approved_amount,
    second_claimed_amount,
    seq_no,
    ROUND(approved_amount / NULLIF(second_claimed_amount, 0), 3) AS approved_amount_per_second_claimed_amount
FROM with_trends
ORDER BY policy_id NULLS LAST, first_claim_date
