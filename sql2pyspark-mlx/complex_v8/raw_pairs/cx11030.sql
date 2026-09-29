WITH cleaned AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM adjusters
    WHERE team = 'Property'
      AND UPPER(TRIM(adjuster_name)) LIKE 'F%'
)
SELECT adjuster_name
FROM cleaned
EXCEPT
SELECT status
FROM claims
