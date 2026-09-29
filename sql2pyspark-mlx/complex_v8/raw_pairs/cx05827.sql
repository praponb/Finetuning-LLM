WITH scoped AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        approved_amount - claimed_amount AS approved_amount_minus_claimed_amount
    FROM claims
    WHERE policy_id >= 19
      AND claim_date < DATE '2025-09-19'
),
with_parent AS (
    -- Attach policy attributes
    SELECT s.*, p.product AS policy_product
    FROM scoped s
    JOIN policies p ON p.id = s.policy_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        approved_amount_minus_claimed_amount,
        MAX(claimed_amount) OVER (PARTITION BY claim_type, adjuster_id ORDER BY approved_amount_minus_claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_claimed_amount_so_far,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY policy_id, claim_type), 2) AS policy_id_claim_type_avg_claimed_amount,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY claim_type ORDER BY claim_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS claimed_amount_avg_3_prior,
        MAX(claimed_amount) OVER (PARTITION BY claim_type, adjuster_id ORDER BY approved_amount_minus_claimed_amount, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS claimed_amount_max_2,
        LEAD(claimed_amount) OVER w_claim_type_adjuster_id_by_ap_3 AS next_claimed_amount,
        ROW_NUMBER() OVER (PARTITION BY claim_type, adjuster_id ORDER BY approved_amount_minus_claimed_amount, id) AS approved_amount_minus_claimed_amount_seq,
        SUM(claimed_amount) OVER w_status AS status_sum_claimed_amount,
        ROUND(PERCENT_RANK() OVER (PARTITION BY claim_type ORDER BY claim_date, id), 3) AS claim_date_percent_rank,
        LAST_VALUE(claimed_amount) IGNORE NULLS OVER (PARTITION BY claim_type, MONTH(claim_date) ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claimed_amount
    FROM with_parent
    WINDOW w_claim_type_adjuster_id_by_ap_3 AS (PARTITION BY claim_type, adjuster_id ORDER BY approved_amount_minus_claimed_amount, id), w_policy_id_claim_type AS (PARTITION BY policy_id, claim_type), w_status AS (PARTITION BY status)
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        claim_date,
        claim_type,
        claimed_amount,
        max_claimed_amount_so_far,
        policy_id_claim_type_avg_claimed_amount,
        claimed_amount_avg_3_prior,
        next_claimed_amount,
        approved_amount_minus_claimed_amount_seq,
        status_sum_claimed_amount,
        claim_date_percent_rank,
        last_known_claimed_amount,
        SUBSTRING(TRIM(status), 1, 3) AS status_prefix
    FROM ranked
    WHERE policy_id_claim_type_avg_claimed_amount < 45010.0
)
SELECT
    id,
    claim_date,
    claim_type,
    claimed_amount,
    max_claimed_amount_so_far,
    policy_id_claim_type_avg_claimed_amount,
    claimed_amount_avg_3_prior,
    next_claimed_amount,
    approved_amount_minus_claimed_amount_seq,
    status_sum_claimed_amount
FROM derived
ORDER BY policy_id_claim_type_avg_claimed_amount, status_sum_claimed_amount NULLS LAST
