WITH all_events AS (
    -- Stack the branches
    SELECT segment, region AS label, 'holder' AS origin
    FROM policyholders
    WHERE risk_score > 0.2
    UNION ALL
    SELECT segment, region AS label, 'holder_2' AS origin
    FROM policyholders
    WHERE joined_on >= DATE '2024-03-22'
      AND region <> 'East'
    UNION ALL
    SELECT claim_type AS segment, status AS label, 'claim' AS origin
    FROM claims
    WHERE claim_type IN ('fire', 'collision')
      AND claim_date BETWEEN DATE '2024-06-20' AND DATE '2025-11-29'
)
SELECT origin, COUNT(*) AS n_rows
FROM all_events
GROUP BY origin
ORDER BY origin
