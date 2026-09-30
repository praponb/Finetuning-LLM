WITH prepared_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        ROUND(premium * 1.5, 2) AS premium_adj,
        ROUND(premium * 1.07, 2) AS premium_adj_2
    FROM policies
    WHERE end_date >= DATE '2026-02-16'
),
enriched AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        s.status,
        p.segment AS holder_segment,
        p.risk_score AS holder_risk_score,
        p.joined_on AS holder_joined_on
    FROM prepared_policys s
    LEFT JOIN policyholders p ON p.id = s.holder_id
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(status, 'ALL') AS status,
    holder_id,
    COALESCE(holder_segment, 'ALL') AS holder_segment,
    COUNT(*) AS cnt,
    PERCENTILE_APPROX(premium, 0.5) AS median_premium,
    ROUND(SUM(premium) / NULLIF(SUM(holder_risk_score), 0), 3) AS premium_to_holder_risk_score_ratio
FROM enriched
GROUP BY GROUPING SETS ((status, holder_id, holder_segment), (status))
ORDER BY status NULLS LAST, holder_id NULLS LAST, holder_segment NULLS LAST
