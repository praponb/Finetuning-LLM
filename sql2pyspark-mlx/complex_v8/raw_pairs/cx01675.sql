WITH cleaned AS (
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
        reported_at,
        ROUND(claimed_amount / NULLIF(approved_amount, 0), 3) AS claimed_amount_per_approved_amount
    FROM claims
    WHERE claim_type = 'collision'
),
summary AS (
    -- Summarise per group
    SELECT
        status,
        MIN(reported_at) AS first_reported_at,
        SUM(approved_amount) AS total_approved_amount
    FROM cleaned
    GROUP BY 1
),
labeled AS (
    -- Derive helper columns
    SELECT *, LOWER(TRIM(status)) AS status_clean
    FROM summary
    WHERE status NOT IN ('APPROVED', 'REJECTED')
)
SELECT
    status,
    first_reported_at,
    total_approved_amount,
    status_clean
FROM labeled
WHERE first_reported_at >= TIMESTAMP '2025-01-31 11:00:00'
ORDER BY first_reported_at DESC
