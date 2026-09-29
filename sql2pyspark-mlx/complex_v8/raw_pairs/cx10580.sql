WITH combined AS (
    -- Stack the branches
    SELECT 'claim' AS record_type, policy_id AS entity_id, claimed_amount AS amount
    FROM claims
    UNION
    SELECT 'holder' AS record_type, id, risk_score
    FROM policyholders
    WHERE risk_score < 0.8
)
SELECT record_type, COUNT(*) AS n_rows, SUM(entity_id) AS total_entity_id
FROM combined
GROUP BY record_type
ORDER BY record_type
