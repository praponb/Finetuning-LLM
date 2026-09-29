WITH cleaned_holders AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        ABS(risk_score - 0.6) AS risk_score_distance
    FROM policyholders
    WHERE risk_score > 0.1
      AND joined_on IS NOT NULL
),
line_level_policys AS (
    -- One row per policy
    SELECT
        s.id,
        s.region,
        s.risk_score,
        s.joined_on,
        s.segment,
        s.risk_score_distance,
        p.id AS policy_id,
        p.product AS policy_product,
        p.premium AS policy_premium,
        p.status AS policy_status,
        p.start_date AS policy_start_date
    FROM cleaned_holders s
    JOIN policies p ON p.holder_id = s.id
)
SELECT
    policy_product,
    segment,
    policy_status,
    ROUND(100 * COUNT(CASE WHEN policy_status NOT IN ('LAPSED', 'CANCELLED') THEN 1 END) / COUNT(*), 1) AS pct_policy_status_not_in_lapsed
FROM line_level_policys
GROUP BY GROUPING SETS ((policy_product, segment, policy_status), (policy_product))
