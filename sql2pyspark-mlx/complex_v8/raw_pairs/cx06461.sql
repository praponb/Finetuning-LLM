WITH recent_holders AS (
    -- Scope the policyholders rows
    SELECT
        id,
        risk_score,
        joined_on,
        segment
    FROM policyholders
    WHERE region <> 'Central'
),
screened AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM recent_holders s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.id
              AND p.start_date < DATE '2025-02-13'
              AND p.product = 'AUTO'
        )
)
-- Final projection
SELECT
    id,
    risk_score,
    segment,
    COALESCE(segment, 'UNKNOWN') AS segment_or_unknown
FROM screened
ORDER BY risk_score, id
LIMIT 10
