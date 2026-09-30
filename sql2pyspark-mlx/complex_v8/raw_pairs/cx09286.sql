WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        GREATEST(approved_amount, claimed_amount) AS greatest_approved_amount_claimed_amount
    FROM claims
    WHERE policy_id IS NOT NULL
),
combined_details_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        s.status,
        s.greatest_approved_amount_claimed_amount,
        p.status AS policy_status,
        p.end_date AS policy_end_date,
        p.product AS policy_product,
        po.segment AS holder_segment,
        po.risk_score AS holder_risk_score
    FROM base s
    JOIN policies p ON p.id = s.policy_id
    LEFT JOIN policyholders po ON po.id = p.holder_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY holder_segment ORDER BY greatest_approved_amount_claimed_amount, id) AS rn,
        MIN(holder_risk_score) OVER w_policy_id_adjuster_id AS policy_id_adjuster_id_min_holder_risk_score,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER w_policy_id, 0), 4) AS approved_amount_share_of_policy_id,
        LEAD(approved_amount, 1, 0) OVER (ORDER BY greatest_approved_amount_claimed_amount NULLS LAST, id) AS next_approved_amount,
        ROUND(holder_risk_score - AVG(holder_risk_score) OVER w_policy_id_adjuster_id, 2) AS holder_risk_score_vs_policy_id_adjuster_id_avg,
        ROW_NUMBER() OVER (ORDER BY greatest_approved_amount_claimed_amount NULLS LAST, id) AS rn_2,
        FIRST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY policy_product ORDER BY holder_risk_score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claim_date,
        LAST_VALUE(holder_risk_score) OVER (ORDER BY greatest_approved_amount_claimed_amount NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_holder_risk_score
    FROM combined_details_policy
    WINDOW w_policy_id_adjuster_id AS (PARTITION BY policy_id, adjuster_id), w_policy_id AS (PARTITION BY policy_id)
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        LEAST(next_approved_amount, rn_2) AS least_next_approved_amount_rn_2,
        CONCAT_WS('-', policy_product, status) AS policy_product_status_label
    FROM windowed
    WHERE next_approved_amount > 2349.3
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claim_type,
    approved_amount,
    status,
    greatest_approved_amount_claimed_amount,
    policy_end_date,
    holder_segment,
    holder_risk_score,
    rn,
    policy_id_adjuster_id_min_holder_risk_score,
    next_approved_amount,
    rn_2,
    first_known_claim_date,
    last_holder_risk_score,
    least_next_approved_amount_rn_2,
    policy_product_status_label,
    COALESCE(policy_status, 'UNKNOWN') AS policy_status_or_unknown
FROM derived
