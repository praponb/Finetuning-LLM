WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        reported_at,
        LOWER(TRIM(status)) AS status_clean
    FROM claims
    WHERE adjuster_id >= 2
      AND policy_id IS NOT NULL
),
combined AS (
    -- Stack the branches
    SELECT 'claim' AS source, policy_id AS key_id, reported_at
    FROM valid
    WHERE UPPER(TRIM(status_clean)) LIKE 'O%'
    UNION ALL
    SELECT 'claim_2' AS source, id AS key_id, reported_at
    FROM claims
)
SELECT source, COUNT(*) AS n_rows, SUM(key_id) AS total_key_id, MAX(reported_at) AS latest_reported_at
FROM combined
GROUP BY source
ORDER BY source
