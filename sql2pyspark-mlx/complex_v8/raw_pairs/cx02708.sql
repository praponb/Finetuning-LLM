WITH filtered AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        status,
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        FLOOR(premium / 1000) * 1000 AS premium_bucket_2
    FROM policies
    WHERE start_date >= DATE '2024-08-27'
      AND holder_id IS NOT NULL
),
qualified AS (
    -- Keep rows whose holder matches
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policyholders p
            WHERE p.id = s.holder_id
              AND YEAR(p.joined_on) = 2024
        )
)
SELECT *
FROM (
    SELECT holder_id, product, status, premium
    FROM qualified
) src
PIVOT (
    MAX(premium) AS max_v, COUNT(premium) AS n
    FOR status IN ('LAPSED', 'ACTIVE', 'CANCELLED')
)
