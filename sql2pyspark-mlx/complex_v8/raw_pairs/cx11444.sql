WITH base_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM policies
    WHERE holder_id IS NOT NULL
)
SELECT product AS label
FROM base_policys
INTERSECT
SELECT claim_type
FROM claims
WHERE adjuster_id IS NOT NULL
