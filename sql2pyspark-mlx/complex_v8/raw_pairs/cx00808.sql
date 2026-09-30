WITH prepared_adjusters AS (
    -- Filter adjusters
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM adjusters
    WHERE LOWER(adjuster_name) LIKE '%ca%'
)
SELECT id AS entity_id
FROM prepared_adjusters
INTERSECT
SELECT id AS entity_id
FROM policyholders
ORDER BY entity_id DESC
