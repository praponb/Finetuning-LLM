WITH recent_policys AS (
    -- Scope the policies rows
    SELECT *
    FROM policies
    WHERE premium > 2013.8
),
joined AS (
    -- Attach holder attributes
    SELECT
        s.*,
        p.region AS holder_region,
        p.holder_name,
        p.joined_on AS holder_joined_on
    FROM recent_policys s
    LEFT JOIN policyholders p ON p.id = s.holder_id
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    premium,
    start_date,
    end_date,
    holder_name,
    holder_joined_on
FROM joined
WHERE holder_id >= 13
ORDER BY premium NULLS LAST
