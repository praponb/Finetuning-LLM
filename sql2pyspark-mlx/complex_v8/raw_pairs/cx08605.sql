WITH filtered AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM adjusters
    WHERE LENGTH(TRIM(adjuster_name)) >= 8
)
SELECT 'adjuster' AS record_type, id AS entity_id, adjuster_name AS label
FROM filtered
WHERE UPPER(TRIM(adjuster_name)) LIKE 'H%'
UNION ALL
SELECT 'claim' AS record_type, adjuster_id AS entity_id, claim_type AS label
FROM claims
WHERE approved_amount BETWEEN 0.0 AND 32878.5
  AND claim_date BETWEEN DATE '2024-03-16' AND DATE '2025-04-17'
UNION ALL
SELECT 'claim' AS record_type, id AS entity_id, claim_type AS label
FROM claims
