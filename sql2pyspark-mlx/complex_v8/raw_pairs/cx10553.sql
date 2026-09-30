WITH scoped AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        segment,
        CASE WHEN risk_score >= 0.8 THEN 'HIGH' WHEN risk_score >= 0.3 THEN 'MEDIUM' ELSE 'LOW' END AS risk_score_tier
    FROM policyholders
    WHERE holder_name IS NOT NULL
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(region, '#', CAST(id AS STRING)) AS region_ref,
        CONCAT_WS(': ', region, segment) AS region_segment_label,
        COALESCE(risk_score, 0.0) AS risk_score_filled
    FROM scoped
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        segment,
        region_ref,
        region_segment_label,
        risk_score_filled,
        MAX(risk_score) OVER (ORDER BY risk_score DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_risk_score_so_far,
        LAG(risk_score) OVER (PARTITION BY risk_score_tier, segment ORDER BY risk_score_filled ASC, id) AS prev_risk_score
    FROM enriched
),
exploded_detail AS (
    -- One row per policy
    SELECT
        s.id,
        s.holder_name,
        s.risk_score,
        s.segment,
        s.region_ref,
        s.region_segment_label,
        s.risk_score_filled,
        p.id AS policy_id,
        p.product AS policy_product,
        p.status AS policy_status,
        p.end_date AS policy_end_date
    FROM with_trends s
    JOIN policies p ON p.holder_id = s.id
)
-- Subtotals with GROUPING SETS
SELECT
    policy_product,
    policy_status,
    ROUND(100 * COUNT(CASE WHEN policy_status = 'EXPIRED' THEN 1 END) / COUNT(*), 1) AS pct_policy_status_expired
FROM exploded_detail
GROUP BY GROUPING SETS ((policy_product, policy_status), (policy_product), ())
ORDER BY policy_product NULLS LAST, policy_status NULLS LAST
