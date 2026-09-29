WITH prepared_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE end_date BETWEEN DATE '2025-04-17' AND DATE '2026-12-05'
      AND premium <= 3465.4
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.end_date,
        s.status,
        p.risk_score AS holder_risk_score,
        p.holder_name,
        p.joined_on AS holder_joined_on
    FROM prepared_policys s
    LEFT JOIN policyholders p ON p.id = s.holder_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT status, product, holder_risk_score
    FROM with_parent
) src
PIVOT (
    SUM(holder_risk_score)
    FOR product IN ('HOME', 'HEALTH', 'AUTO')
)
