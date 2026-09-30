WITH scoped_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date
    FROM policies
)
SELECT id AS entity_id, start_date AS event_date, 'policy' AS source
FROM scoped_policys
WHERE end_date IS NOT NULL
UNION ALL
SELECT id, joined_on, 'holder' AS source
FROM policyholders
UNION ALL
SELECT holder_id, start_date, 'policy_3' AS source
FROM policies
