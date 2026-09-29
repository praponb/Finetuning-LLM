WITH recent_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        reported_at,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref
    FROM claims
    WHERE claim_type <> 'delay'
),
combined AS (
    -- Stack the branches
    SELECT 'claim' AS origin, id, reported_at
    FROM recent_claims
    WHERE status_ref IS NOT NULL
      AND policy_id IS NOT NULL
    UNION
    SELECT 'claim_2' AS origin, policy_id AS id, reported_at
    FROM claims
    WHERE claim_date >= DATE '2024-05-14'
)
SELECT origin, COUNT(*) AS n_rows, SUM(id) AS total_id, MAX(reported_at) AS latest_reported_at
FROM combined
GROUP BY origin
ORDER BY origin
