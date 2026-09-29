WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM adjusters
    WHERE (LENGTH(TRIM(adjuster_name)) >= 7 OR team IN ('Property', 'Travel', 'Health'))
)
SELECT id AS ref_id, adjuster_name AS name
FROM prepared
WHERE team = 'Travel'
INTERSECT ALL
SELECT policy_id, claim_type
FROM claims
