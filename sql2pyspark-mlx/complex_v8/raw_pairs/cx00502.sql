WITH recent AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE premium < 4417.0
),
joined AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.end_date,
        s.status,
        p.region AS holder_region,
        p.risk_score AS holder_risk_score,
        p.segment AS holder_segment
    FROM recent s
    JOIN policyholders p ON p.id = s.holder_id AND p.joined_on BETWEEN DATE '2024-06-18' AND DATE '2025-05-01'
    WHERE p.segment = 'SME'
),
screened AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM joined s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.adjuster_id IS NOT NULL
              AND c.status NOT IN ('OPEN', 'IN_REVIEW')
        )
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT holder_region, status, premium
    FROM screened
) src
PIVOT (
    AVG(premium)
    FOR status IN ('CANCELLED', 'EXPIRED')
)
