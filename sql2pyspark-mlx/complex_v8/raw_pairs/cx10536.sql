WITH combined AS (
    -- Stack the branches
    SELECT id AS entity_id, claim_date AS day, policy_id AS entity_id_2
    FROM claims
    UNION
    SELECT policy_id, claim_date, id
    FROM claims
    WHERE claim_date < DATE '2025-04-25'
)
SELECT entity_id, COUNT(*) AS n_rows, SUM(entity_id_2) AS total_entity_id_2, MAX(day) AS latest_day
FROM combined
GROUP BY entity_id
ORDER BY entity_id
