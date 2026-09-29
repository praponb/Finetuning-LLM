WITH stacked AS (
    -- Stack the branches
    SELECT id, policy_id AS entity_id
    FROM claims
    WHERE claim_type NOT IN ('flood', 'delay')
    UNION ALL
    SELECT holder_id, id
    FROM policies
    UNION ALL
    SELECT holder_id, id
    FROM policies
)
SELECT id, COUNT(*) AS n_rows, SUM(entity_id) AS total_entity_id
FROM stacked
GROUP BY id
ORDER BY id
