WITH scoped AS (
    -- Scope the policies rows
    SELECT *
    FROM policies
    WHERE premium > 1042.5
      AND product = 'HOME'
),
product_stats AS (
    -- Avg premium per product
    SELECT product, AVG(premium) AS avg_premium
    FROM scoped
    GROUP BY product
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_premium,
        ROUND(s.premium - g.avg_premium, 2) AS premium_minus_avg
    FROM scoped s
    JOIN product_stats g ON g.product = s.product
    WHERE s.premium > g.avg_premium
),
holder_id_stats AS (
    -- Max premium_minus_avg per holder_id
    SELECT holder_id, MAX(premium_minus_avg) AS max_premium_minus_avg
    FROM outliers
    GROUP BY holder_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_premium_minus_avg,
        ROUND(s.premium_minus_avg - g.max_premium_minus_avg, 2) AS premium_minus_avg_minus_max
    FROM outliers s
    JOIN holder_id_stats g ON g.holder_id = s.holder_id
    WHERE s.premium_minus_avg = g.max_premium_minus_avg
)
SELECT id, measure, metric_value
FROM (
    SELECT id, premium_minus_avg, premium, avg_premium
    FROM vs_group
) src
UNPIVOT (
    metric_value FOR measure IN (premium_minus_avg, premium, avg_premium)
) unp
