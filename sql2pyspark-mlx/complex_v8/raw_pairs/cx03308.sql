WITH valid AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        ROUND(premium * 1.1, 2) AS premium_adj,
        CASE WHEN premium >= 3719.7 THEN 'large' WHEN premium >= 2013.8 THEN 'medium' ELSE 'small' END AS premium_tier,
        LOWER(TRIM(product)) AS product_clean
    FROM policies
    WHERE end_date >= DATE '2025-09-27'
      AND product IS NOT NULL
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.status,
        s.premium_adj,
        s.premium_tier,
        s.product_clean,
        p.holder_name,
        p.joined_on AS holder_joined_on
    FROM valid s
    JOIN policyholders p ON p.id = s.holder_id
),
product_stats AS (
    -- Avg premium_adj per product
    SELECT product, AVG(premium_adj) AS avg_premium_adj
    FROM with_details
    GROUP BY product
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_premium_adj,
        ROUND(s.premium_adj - g.avg_premium_adj, 2) AS premium_adj_minus_avg
    FROM with_details s
    JOIN product_stats g ON g.product = s.product
    WHERE s.premium_adj > g.avg_premium_adj
)
SELECT
    premium_tier,
    status,
    holder_id,
    ARRAY_SORT(COLLECT_SET(product)) AS product_list,
    MIN(holder_joined_on) AS first_holder_joined_on,
    COUNT(DISTINCT id) AS distinct_ids
FROM compared
GROUP BY GROUPING SETS ((premium_tier, status, holder_id), (premium_tier), ())
ORDER BY premium_tier NULLS LAST, status NULLS LAST, holder_id NULLS LAST
