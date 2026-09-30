WITH recent_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown
    FROM policies
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM recent_policys s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.status IN ('PAID', 'REJECTED', 'OPEN')
              AND c.claim_date BETWEEN DATE '2024-06-07' AND DATE '2025-10-15'
        )
),
product_or_unknown_stats AS (
    -- Avg premium per product_or_unknown
    SELECT product_or_unknown, AVG(premium) AS avg_premium
    FROM screened
    GROUP BY product_or_unknown
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_premium,
        ROUND(s.premium - g.avg_premium, 2) AS premium_minus_avg
    FROM screened s
    JOIN product_or_unknown_stats g ON g.product_or_unknown = s.product_or_unknown
    WHERE s.premium > g.avg_premium
),
stats AS (
    -- Group-level totals
    SELECT
        product_or_unknown,
        ROUND(STDDEV(premium), 2) AS stddev_premium,
        ROUND(SUM(avg_premium), 2) AS total_avg_premium,
        COUNT(*) AS row_count,
        ROUND(SUM(avg_premium) / NULLIF(SUM(premium_minus_avg), 0), 3) AS avg_premium_to_premium_minus_avg_ratio,
        COUNT(DISTINCT product) AS distinct_products
    FROM vs_group
    GROUP BY product_or_unknown
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM stats
    WHERE row_count < (SELECT MAX(row_count) FROM stats)
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM without_matches
    WHERE total_avg_premium < (SELECT MAX(total_avg_premium) FROM without_matches)
),
without_matches_2 AS (
    -- Compare against the overall avg
    SELECT *
    FROM filtered
    WHERE distinct_products > (SELECT AVG(distinct_products) FROM filtered)
)
SELECT
    product_or_unknown,
    stddev_premium,
    total_avg_premium,
    avg_premium_to_premium_minus_avg_ratio,
    FLOOR(total_avg_premium / 5000) * 5000 AS total_avg_premium_bucket
FROM without_matches_2
WHERE stddev_premium >= 1250.0
ORDER BY product_or_unknown DESC NULLS FIRST, stddev_premium
