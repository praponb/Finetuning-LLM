WITH valid_holders AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        region,
        segment,
        CONCAT(segment, '#', CAST(id AS STRING)) AS segment_ref
    FROM policyholders
    WHERE joined_on IS NOT NULL
)
SELECT id AS entity_id, region AS category
FROM valid_holders
WHERE segment_ref IS NOT NULL
  AND region <> 'West'
INTERSECT
SELECT id AS entity_id, claim_type AS category
FROM claims
