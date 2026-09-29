WITH scoped AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount
    FROM claims
    WHERE (HOUR(reported_at) BETWEEN 10 AND 15 OR claim_type = 'fire')
),
unioned AS (
    -- Stack the branches
    SELECT status AS name, reported_at
    FROM claims
    WHERE status = 'APPROVED'
    UNION
    SELECT status, reported_at
    FROM claims
    WHERE claimed_amount >= 20436.5
)
SELECT name, COUNT(*) AS n_rows, MAX(reported_at) AS latest_reported_at
FROM unioned
GROUP BY name
ORDER BY name
