WITH recent_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATE_TRUNC('WEEK', start_date) AS start_date_week
    FROM policies
    WHERE status IN ('EXPIRED', 'CANCELLED')
      AND start_date IS NOT NULL
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        s.start_date_week,
        p.risk_score AS holder_risk_score,
        p.holder_name
    FROM recent_policys s
    JOIN policyholders p ON p.id = s.holder_id AND YEAR(p.joined_on) = 2024
    WHERE p.holder_name IS NOT NULL
),
with_parent AS (
    -- Attach holder attributes
    SELECT s.*, p.risk_score AS holder_risk_score_2
    FROM joined s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.risk_score BETWEEN 0.1 AND 0.6
)
SELECT id, metric_name, reading
FROM (
    SELECT id, premium, holder_risk_score, holder_risk_score_2
    FROM with_parent
) src
UNPIVOT (
    reading FOR metric_name IN (premium, holder_risk_score, holder_risk_score_2)
) unp
