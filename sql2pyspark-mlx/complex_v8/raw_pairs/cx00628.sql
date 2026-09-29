WITH prepared_claims AS (
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
        ABS(claimed_amount - 19337.6) AS claimed_amount_distance,
        DATEDIFF(DATE '2025-06-30', reported_at) AS days_since_reported_at,
        UPPER(TRIM(status)) AS status_clean
    FROM claims
    WHERE claimed_amount >= 19609.8
),
filtered AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM prepared_claims s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.approved_amount >= 2349.3
        )
)
SELECT id, field, reading
FROM (
    SELECT id, claimed_amount_distance, claimed_amount
    FROM filtered
) src
UNPIVOT (
    reading FOR field IN (claimed_amount_distance, claimed_amount)
) unp
ORDER BY id, field
