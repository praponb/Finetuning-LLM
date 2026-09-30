WITH cleaned_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        CASE WHEN premium >= 2945.9 THEN 'A' WHEN premium >= 2096.6 THEN 'B' ELSE 'C' END AS premium_tier
    FROM policies
    WHERE status IS NOT NULL
      AND holder_id >= 5
),
status_stats AS (
    -- Avg premium per status
    SELECT status, AVG(premium) AS avg_premium
    FROM cleaned_policys
    GROUP BY status
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_premium,
        ROUND(s.premium - g.avg_premium, 2) AS premium_minus_avg
    FROM cleaned_policys s
    JOIN status_stats g ON g.status = s.status
    WHERE s.premium > g.avg_premium
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        product,
        start_date,
        premium_tier,
        avg_premium,
        premium_minus_avg,
        COALESCE(avg_premium, 0.0) AS avg_premium_filled,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown,
        premium_minus_avg - avg_premium AS premium_minus_avg_minus_avg_premium
    FROM outliers
    WHERE avg_premium BETWEEN 1160.0 AND 3320.0
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(product, 'ALL') AS product,
    COALESCE(product_or_unknown, 'ALL') AS product_or_unknown,
    COALESCE(premium_tier, 'ALL') AS premium_tier,
    GROUPING_ID() AS grouping_level,
    COUNT(*) AS row_count
FROM tagged
GROUP BY GROUPING SETS ((product, product_or_unknown, premium_tier), (product), ())
