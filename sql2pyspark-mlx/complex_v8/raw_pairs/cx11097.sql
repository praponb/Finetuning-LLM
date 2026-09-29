WITH all_events AS (
    -- Stack the branches
    SELECT
        policy_id,
        claimed_amount,
        adjuster_id,
        'claim' AS src
    FROM claims
    WHERE TO_DATE(reported_at) < DATE '2025-03-14'
      AND policy_id IS NOT NULL
    UNION ALL
    SELECT
        id,
        claimed_amount,
        policy_id,
        'claim_2' AS src
    FROM claims
    WHERE adjuster_id IS NOT NULL
)
SELECT src, COUNT(*) AS n_rows, SUM(claimed_amount) AS total_claimed_amount
FROM all_events
GROUP BY src
ORDER BY src
