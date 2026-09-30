WITH filtered_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        reported_at
    FROM claims
    WHERE HOUR(reported_at) BETWEEN 6 AND 11
      AND approved_amount BETWEEN 0.0 AND 24718.5
)
SELECT
    'claim' AS source,
    status AS category,
    claim_type AS name,
    reported_at
FROM filtered_claims
UNION ALL
SELECT
    'claim_2' AS source,
    status AS category,
    claim_type AS name,
    reported_at
FROM claims
UNION ALL
SELECT
    'claim_3' AS source,
    status AS category,
    claim_type AS name,
    reported_at
FROM claims
