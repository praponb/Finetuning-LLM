WITH filtered_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        reported_at,
        TO_DATE(reported_at) AS reported_at_date,
        LENGTH(TRIM(status)) AS status_len
    FROM claims
    WHERE status = 'REJECTED'
      AND approved_amount < 24718.5
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM filtered_claims
    WHERE claimed_amount < (SELECT MAX(claimed_amount) FROM filtered_claims)
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        reported_at,
        reported_at_date,
        status_len,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown
    FROM with_activity
    WHERE policy_id IS NOT NULL
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        reported_at,
        reported_at_date,
        claim_type_or_unknown,
        ABS(status_len - 6) AS status_len_distance,
        LOWER(TRIM(claim_type_or_unknown)) AS claim_type_or_unknown_clean
    FROM derived
    WHERE claim_type_or_unknown IN ('delay', 'UNKNOWN', 'flood')
),
without_matches AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM labeled s
    WHERE s.policy_id IN (
            SELECT c.policy_id
            FROM claims c
            WHERE c.claimed_amount <= 30115.9
              AND c.claim_date IS NOT NULL
        )
)
SELECT
    id,
    policy_id,
    adjuster_id,
    reported_at,
    reported_at_date,
    claim_type_or_unknown,
    status_len_distance,
    claim_type_or_unknown_clean,
    ROUND(claimed_amount * 0.9, 2) AS claimed_amount_adj,
    DATE_TRUNC('YEAR', reported_at_date) AS reported_at_date_year
FROM without_matches
WHERE reported_at_date < DATE '2025-05-01'
ORDER BY reported_at_date_year NULLS LAST
