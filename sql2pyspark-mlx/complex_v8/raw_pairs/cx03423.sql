WITH unioned AS (
    -- Stack the branches
    SELECT adjuster_id AS key_id, approved_amount AS metric, claim_date
    FROM claims
    UNION ALL
    SELECT id, risk_score, joined_on
    FROM policyholders
    WHERE holder_name IS NOT NULL
      AND region IN ('West', 'Central', 'East')
    UNION ALL
    SELECT id, claimed_amount, claim_date
    FROM claims
)
SELECT key_id, COUNT(*) AS n_rows, SUM(metric) AS total_metric, MAX(claim_date) AS latest_claim_date
FROM unioned
GROUP BY key_id
ORDER BY key_id
