WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        LENGTH(TRIM(product)) AS product_len
    FROM policies
    WHERE product = 'AUTO'
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT
        holder_id,
        COUNT(*) FILTER (WHERE status = 'ACTIVE') AS policy_status_count
    FROM policies
    GROUP BY holder_id
),
with_stats AS (
    -- Attach policies metrics
    SELECT
        s.*,
        COALESCE(p.policy_status_count, 0) AS policy_status_count
    FROM prepared s
    LEFT JOIN policy_stats p ON p.holder_id = s.holder_id
),
joined AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        s.status,
        s.product_len,
        p.region AS holder_region
    FROM with_stats s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.region IN ('Central', 'South', 'West')
),
product_stats AS (
    -- Avg premium per product
    SELECT product, AVG(premium) AS avg_premium
    FROM joined
    GROUP BY product
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_premium,
        ROUND(s.premium - g.avg_premium, 2) AS premium_minus_avg
    FROM joined s
    JOIN product_stats g ON g.product = s.product
    WHERE s.premium > g.avg_premium
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    start_date,
    end_date,
    status,
    product_len,
    holder_region,
    avg_premium
FROM outliers
WHERE start_date < DATE '2025-03-23'
ORDER BY product_len, product
