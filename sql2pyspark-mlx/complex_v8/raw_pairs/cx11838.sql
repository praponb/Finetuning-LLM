WITH base_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        reported_at,
        ABS(approved_amount - 15857.9) AS approved_amount_distance,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled,
        UPPER(TRIM(claim_type)) AS claim_type_clean
    FROM claims
    WHERE claim_date IS NOT NULL
      AND status NOT IN ('OPEN', 'PAID')
),
joined AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.status,
        s.approved_amount_distance,
        s.claimed_amount_filled,
        s.claim_type_clean,
        a.adjuster_name,
        a.level AS adjuster_level,
        a.team AS adjuster_team
    FROM base_claims s
    JOIN adjusters a ON a.id = s.adjuster_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(c.approved_amount), 2) FROM claims c WHERE c.policy_id = s.policy_id) AS avg_claim_approved_amount,
        (SELECT MAX(c.claim_date) FROM claims c WHERE c.policy_id = s.policy_id) AS last_claim_date,
        (SELECT COUNT(*) FILTER (WHERE c.status IS NOT NULL) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_status_count
    FROM joined s
)
SELECT
    id,
    policy_id,
    adjuster_id,
    claimed_amount,
    status,
    claimed_amount_filled,
    adjuster_name,
    adjuster_level,
    avg_claim_approved_amount,
    last_claim_date,
    claim_status_count,
    LENGTH(TRIM(claim_type_clean)) AS claim_type_clean_len,
    claimed_amount - approved_amount_distance AS claimed_amount_minus_approved_amount_distance
FROM with_counts
ORDER BY adjuster_id NULLS LAST, id
LIMIT 20
