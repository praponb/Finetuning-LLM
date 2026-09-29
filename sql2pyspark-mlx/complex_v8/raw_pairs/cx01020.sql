WITH cleaned_policys AS (
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
    WHERE status IN ('CANCELLED', 'ACTIVE')
      AND start_date >= DATE '2024-12-03'
),
filtered AS (
    -- Keep rows whose holder matches
    SELECT s.*
    FROM cleaned_policys s
    WHERE EXISTS (
            SELECT 1
            FROM policyholders p
            WHERE p.id = s.holder_id
              AND p.segment NOT IN ('RETAIL', 'VIP')
              AND p.risk_score BETWEEN 0.2 AND 0.6
        )
)
SELECT
    product,
    holder_id,
    GROUPING_ID() AS grouping_level,
    COUNT(*) AS n_rows,
    PERCENTILE_APPROX(premium, 0.5) AS median_premium,
    SUM(premium) FILTER (WHERE product NOT IN ('AUTO', 'HEALTH')) AS premium_product_not_in_auto_health
FROM filtered
GROUP BY ROLLUP(product, holder_id)
