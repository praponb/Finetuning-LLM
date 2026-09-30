WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        LEAST(approved_amount, claimed_amount) AS least_approved_amount_claimed_amount,
        CONCAT(status, '#', CAST(adjuster_id AS STRING)) AS status_ref
    FROM claims
    WHERE MONTH(reported_at) IN (2, 3)
      AND approved_amount >= 0.0
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MAX(c.claimed_amount) FROM claims c WHERE c.policy_id = s.policy_id) AS max_claim_claimed_amount,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_count
    FROM prepared s
),
labeled AS (
    -- Label and score rows
    SELECT *, DATEDIFF(DATE '2025-06-30', claim_date) AS days_since_claim_date
    FROM with_counts
)
SELECT *
FROM (
    SELECT status, claim_date, claim_type, least_approved_amount_claimed_amount
    FROM labeled
) src
PIVOT (
    SUM(least_approved_amount_claimed_amount)
    FOR claim_type IN ('delay', 'theft', 'flood')
)
