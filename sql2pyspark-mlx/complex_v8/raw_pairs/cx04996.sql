WITH valid AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        CONCAT(status, '#', CAST(holder_id AS STRING)) AS status_ref
    FROM policies
    WHERE product = 'LIFE'
      AND premium <= 3532.7
),
joined_holder AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        p.holder_name,
        p.segment AS holder_segment,
        p.risk_score AS holder_risk_score
    FROM valid s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
qualified AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM joined_holder s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.reported_at >= TIMESTAMP '2025-01-31 09:00:00'
              AND c.approved_amount > 1273.7
        )
      AND s.holder_id IS NOT NULL
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT start_date, holder_id, product, premium
    FROM qualified
) src
PIVOT (
    AVG(premium)
    FOR product IN ('AUTO', 'LIFE', 'HOME')
)
