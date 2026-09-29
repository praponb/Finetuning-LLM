WITH unioned AS (
    -- Stack the branches
    SELECT 'claim' AS origin, policy_id AS entity_id, reported_at
    FROM claims
    WHERE claim_date IS NOT NULL
      AND approved_amount <= 14993.0
    UNION ALL
    SELECT 'claim_2' AS origin, adjuster_id, reported_at
    FROM claims
    WHERE policy_id IS NOT NULL
      AND approved_amount < 32511.8
)
SELECT origin, COUNT(*) AS n_rows, SUM(entity_id) AS total_entity_id, MAX(reported_at) AS latest_reported_at
FROM unioned
GROUP BY origin
ORDER BY origin
