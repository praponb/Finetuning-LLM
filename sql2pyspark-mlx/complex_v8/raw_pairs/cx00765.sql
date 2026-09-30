WITH base_adjusters AS (
    -- Filter adjusters
    SELECT
        id,
        adjuster_name,
        team,
        level,
        LENGTH(TRIM(adjuster_name)) AS adjuster_name_len
    FROM adjusters
    WHERE UPPER(TRIM(adjuster_name)) LIKE 'B%'
)
SELECT id AS ref_id
FROM base_adjusters
INTERSECT
SELECT id
FROM policyholders
WHERE region <> 'South'
ORDER BY ref_id DESC
