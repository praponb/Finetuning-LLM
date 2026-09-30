WITH cleaned_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount
    FROM claims
    WHERE reported_at < TIMESTAMP '2025-02-18 15:00:00'
),
unioned AS (
    -- Stack the branches
    SELECT claim_type, claim_date AS day
    FROM cleaned_claims
    UNION ALL
    SELECT claim_type, claim_date
    FROM claims
)
SELECT claim_type, COUNT(*) AS n_rows, MAX(day) AS latest_day
FROM unioned
GROUP BY claim_type
ORDER BY claim_type
