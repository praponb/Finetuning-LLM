WITH cleaned_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        CASE WHEN product IN ('TRAVEL', 'LIFE') THEN 'group_a' WHEN product = 'HOME' THEN 'group_b' ELSE 'other' END AS product_group,
        ABS(premium - 3521.8) AS premium_distance
    FROM policies
    WHERE (end_date >= DATE '2025-12-05' OR holder_id <= 6)
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM cleaned_policys
    WHERE premium > (SELECT AVG(premium) FROM cleaned_policys)
),
unique_rows AS (
    -- Keep the earliest row per product
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        product_group,
        premium_distance
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product ORDER BY premium, id) AS rn
        FROM with_activity
    ) ranked
    WHERE rn = 1
),
with_parent_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.end_date,
        s.status,
        s.product_group,
        s.premium_distance,
        p.risk_score AS holder_risk_score
    FROM unique_rows s
    LEFT JOIN policyholders p ON p.id = s.holder_id
)
-- Subtotals with ROLLUP
SELECT
    product,
    product_group,
    SUM(holder_risk_score) FILTER (WHERE status NOT IN ('EXPIRED', 'ACTIVE')) AS holder_risk_score_status_not_in_expired_active,
    ROUND(SUM(holder_risk_score), 2) AS total_holder_risk_score,
    ROUND(100 * COUNT(CASE WHEN status = 'EXPIRED' THEN 1 END) / COUNT(*), 1) AS pct_status_expired
FROM with_parent_holder
GROUP BY ROLLUP(product, product_group)
ORDER BY product NULLS LAST, product_group NULLS LAST
