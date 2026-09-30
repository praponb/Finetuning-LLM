WITH recent_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        CONCAT(claim_type, '#', CAST(adjuster_id AS STRING)) AS claim_type_ref
    FROM claims
    WHERE reported_at >= TIMESTAMP '2025-01-19 15:00:00'
      AND claimed_amount <= 29336.0
),
joined AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.status,
        s.reported_at,
        s.claim_type_ref,
        p.end_date AS policy_end_date
    FROM recent_claims s
    JOIN policies p ON p.id = s.policy_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(claimed_amount) OVER (ORDER BY claimed_amount ASC NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS claimed_amount_avg_7,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY status ORDER BY claimed_amount DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS claimed_amount_avg_2_prior,
        SUM(claimed_amount) OVER (PARTITION BY status, policy_id) AS status_policy_id_sum_claimed_amount,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY status, policy_id), 0), 4) AS claimed_amount_share_of_status_policy_id
    FROM joined
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.claim_type_ref,
        s.policy_end_date,
        s.status_policy_id_sum_claimed_amount,
        s.claimed_amount_share_of_status_policy_id,
        c.id AS claim_id,
        c.status AS claim_status,
        c.claimed_amount AS claim_claimed_amount
    FROM sequenced s
    LEFT JOIN claims c ON c.adjuster_id = s.adjuster_id
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        claim_claimed_amount - claimed_amount AS claim_claimed_amount_minus_claimed_amount
    FROM exploded_detail_claims
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claimed_amount,
    claim_type_ref,
    status_policy_id_sum_claimed_amount,
    claimed_amount_share_of_status_policy_id,
    claim_id,
    claim_status,
    claim_claimed_amount_minus_claimed_amount,
    QUARTER(policy_end_date) AS policy_end_date_quarter,
    DATE_FORMAT(policy_end_date, 'MMM yyyy') AS policy_end_date_label
FROM labeled
WHERE claim_claimed_amount > 19347.2
