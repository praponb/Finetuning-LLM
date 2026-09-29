WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        segment,
        ABS(risk_score - 0.2) AS risk_score_distance,
        COALESCE(risk_score, 0.0) AS risk_score_filled,
        ROUND(risk_score * 1.1, 2) AS risk_score_adj
    FROM policyholders
    WHERE joined_on >= DATE '2024-07-18'
      AND risk_score >= 0.3
),
line_level_policys AS (
    -- One row per policy
    SELECT
        s.id,
        s.holder_name,
        s.region,
        s.risk_score,
        s.segment,
        s.risk_score_adj,
        p.id AS policy_id,
        p.product AS policy_product,
        p.status AS policy_status
    FROM base s
    LEFT JOIN policies p ON p.holder_id = s.id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(risk_score_adj, 2) OVER (PARTITION BY policy_status, policy_product ORDER BY risk_score, id, policy_id) AS next2_risk_score_adj,
        RANK() OVER w_policy_status_policy_product AS risk_score_rank,
        COUNT(*) OVER (PARTITION BY segment, region) AS rows_in_segment_region
    FROM line_level_policys
    WINDOW w_policy_status_policy_product AS (PARTITION BY policy_status, policy_product ORDER BY risk_score, id, policy_id)
)
SELECT
    id,
    risk_score,
    segment,
    risk_score_adj,
    policy_product,
    policy_status,
    rows_in_segment_region
FROM ranked
WHERE policy_product NOT IN ('TRAVEL', 'HOME')
ORDER BY segment
