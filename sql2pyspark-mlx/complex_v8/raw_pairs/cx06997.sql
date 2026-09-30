WITH valid_claims AS (
    -- Scope the claims rows
    SELECT *
    FROM claims
    WHERE adjuster_id IS NOT NULL
)
SELECT id AS key_id, claim_date AS event_date
FROM valid_claims
UNION ALL
SELECT holder_id AS key_id, end_date AS event_date
FROM policies
WHERE holder_id >= 11
  AND start_date IS NOT NULL
