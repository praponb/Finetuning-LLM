WITH recent_claims AS (
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
        LENGTH(TRIM(status)) AS status_len,
        LOWER(TRIM(status)) AS status_clean
    FROM claims
    WHERE adjuster_id IS NOT NULL
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.status,
        s.status_len,
        s.status_clean,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.claim_type AS claim_claim_type,
        c.claim_date AS claim_claim_date
    FROM recent_claims s
    LEFT JOIN claims c ON c.policy_id = s.policy_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        claim_date,
        claim_type,
        status,
        status_len,
        claim_id,
        claim_approved_amount,
        claim_claim_type,
        claim_claim_date,
        LAST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY claim_type ORDER BY claim_approved_amount, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claim_date,
        NTILE(3) OVER (PARTITION BY claim_type ORDER BY claim_approved_amount, id, claim_id) AS claim_approved_amount_tercile,
        ROUND(AVG(status_len) OVER (PARTITION BY claim_type ORDER BY claim_approved_amount, id, claim_id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS status_len_avg_1_prior,
        SUM(status_len) OVER (PARTITION BY claim_id) AS claim_id_sum_status_len,
        status_len - LAG(status_len) OVER (PARTITION BY claim_type ORDER BY claim_approved_amount, id, claim_id) AS status_len_change,
        DATEDIFF(claim_date, LAG(claim_date) OVER (PARTITION BY claim_type ORDER BY claim_approved_amount, id, claim_id)) AS days_since_prev_claim_date
    FROM with_children
)
-- Subtotals with GROUPING SETS
SELECT
    claim_claim_type,
    claim_type,
    claim_approved_amount_tercile,
    SUM(CASE WHEN status = 'REJECTED' THEN 1 ELSE 0 END) AS n_status_rejected
FROM metrics
GROUP BY GROUPING SETS ((claim_claim_type, claim_type, claim_approved_amount_tercile), (claim_claim_type), (claim_type), ())
ORDER BY claim_claim_type NULLS LAST, claim_type NULLS LAST, claim_approved_amount_tercile NULLS LAST
