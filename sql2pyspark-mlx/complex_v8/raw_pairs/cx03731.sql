WITH scoped AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        DATEDIFF(DATE '2025-06-30', claim_date) AS days_since_claim_date
    FROM claims
    WHERE claim_type IN ('fire', 'collision')
      AND policy_id IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY claim_type ORDER BY claimed_amount DESC, id) AS claimed_amount_quintile,
        MAX(claimed_amount) OVER (ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_claimed_amount_so_far,
        SUM(days_since_claim_date) OVER (ORDER BY claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_days_since_claim_date,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER w_claim_type_adjuster_id, 0), 4) AS approved_amount_share_of_claim_type_adjuster_id,
        FIRST_VALUE(days_since_claim_date) IGNORE NULLS OVER (PARTITION BY claim_type ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_days_since_claim_date,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY claim_type, adjuster_id), 2) AS approved_amount_vs_claim_type_adjuster_id_avg,
        claimed_amount - LAG(claimed_amount) OVER (PARTITION BY claim_type ORDER BY claimed_amount DESC, id) AS claimed_amount_change,
        SUM(approved_amount) OVER (ORDER BY claim_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS approved_amount_sum_4,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status, YEAR(claim_date) ORDER BY claimed_amount, id), 3) AS claimed_amount_percent_rank
    FROM scoped
    WINDOW w_claim_type_adjuster_id AS (PARTITION BY claim_type, adjuster_id)
)
SELECT
    id,
    policy_id,
    claim_date,
    claimed_amount,
    approved_amount,
    status,
    claimed_amount_quintile,
    max_claimed_amount_so_far,
    approved_amount_share_of_claim_type_adjuster_id,
    first_known_days_since_claim_date,
    approved_amount_vs_claim_type_adjuster_id_avg,
    claimed_amount_change,
    approved_amount_sum_4,
    QUARTER(claim_date) AS claim_date_quarter
FROM with_windows
WHERE approved_amount_vs_claim_type_adjuster_id_avg BETWEEN -30000.0 AND 40000.0
ORDER BY approved_amount DESC, claim_date_quarter DESC NULLS FIRST
