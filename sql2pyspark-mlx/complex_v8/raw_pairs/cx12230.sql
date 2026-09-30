WITH recent_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        status,
        CASE WHEN product IN ('TRAVEL', 'AUTO') THEN 'group_a' WHEN product = 'HOME' THEN 'group_b' ELSE 'other' END AS product_group
    FROM policies
    WHERE (premium > 2111.3 OR start_date >= DATE '2024-06-19')
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        COALESCE(premium, 0.0) AS premium_filled
    FROM recent_policys
),
status_stats AS (
    -- Max premium_bucket per status
    SELECT status, MAX(premium_bucket) AS max_premium_bucket
    FROM derived
    GROUP BY status
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_premium_bucket,
        ROUND(s.premium_bucket - g.max_premium_bucket, 2) AS premium_bucket_minus_max
    FROM derived s
    JOIN status_stats g ON g.status = s.status
    WHERE s.premium_bucket = g.max_premium_bucket
)
SELECT
    id,
    holder_id,
    product,
    premium,
    product_group,
    premium_bucket,
    premium_filled,
    max_premium_bucket,
    premium_bucket_minus_max
FROM vs_group
ORDER BY id DESC, premium DESC
